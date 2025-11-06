"""Blueprint routes for authentication flows."""
from __future__ import annotations

from datetime import datetime, timedelta

from flask import abort, current_app, flash, redirect, render_template, request, session, url_for
from flask_login import current_user, login_required, login_user, logout_user

from expenseai_ext.db import db
from expenseai_ext.security import limiter, user_or_ip_rate_limit
from expenseai_models.audit import AuditLog
from expenseai_models.session import SessionToken
from expenseai_models.user import User
from expenseai_auth import auth_bp
from expenseai_auth.forms import ForgotPasswordForm, LoginForm, OtpVerificationForm, RegisterForm, ResetPasswordForm
from expenseai_auth import otp_service
from expenseai_auth.otp_service import (
    OtpAttemptsExceededError,
    OtpError,
    OtpExpiredError,
    OtpNotFoundError,
    OtpThrottleError,
    OtpValidationError,
)
from expenseai_ext.email import send_email
from expenseai_auth.services import OrganizationService, UserService


OTP_PURPOSE_REGISTRATION = "registration"
OTP_PURPOSE_PASSWORD_RESET = "password_reset"
PASSWORD_RESET_SESSION_KEY = "password_reset_context"


def _create_session_record(user, remember: bool) -> SessionToken:
    """Persist a session token to allow future audits."""
    token = SessionToken.issue(user_id=user.id, ip=request.remote_addr, user_agent=request.user_agent.string, remember_me=remember)
    return token


def _mask_email(email: str) -> str:
    """Obscure the local part of an email for display."""
    if "@" not in email:
        return email
    local, domain = email.split("@", 1)
    if len(local) <= 2:
        masked_local = local[0] + "*"
    else:
        masked_local = local[0] + "*" * (len(local) - 2) + local[-1]
    return f"{masked_local}@{domain}"


def _otp_labels(purpose: str) -> tuple[str, str]:
    app_name = current_app.config.get("APP_NAME", "Finvela")
    if purpose == OTP_PURPOSE_PASSWORD_RESET:
        return (f"Reset your {app_name} password", "Reset password")
    return (f"Verify your {app_name} account", "Verify account")


def _otp_email_context(user, otp_code: str, purpose: str, action_url: str, metadata: dict[str, str] | None = None) -> dict[str, object]:
    """Build template context payload shared across email templates."""
    metadata = metadata or {}
    headline, cta_label = _otp_labels(purpose)
    expiry_minutes = int(current_app.config.get("OTP_EXPIRY_MINUTES", 10))
    support_email = current_app.config.get("EMAIL_FROM")
    return {
        "app_name": current_app.config.get("APP_NAME", "Finvela"),
        "name": user.full_name,
        "otp": otp_code,
        "expiry_minutes": expiry_minutes,
        "cta_label": cta_label,
        "headline": headline,
        "action_url": action_url,
        "purpose": purpose,
        "purpose_metadata": metadata,
        "support_email": support_email,
    }


def _dispatch_otp_email(
    user: User,
    *,
    otp_code: str,
    purpose: str,
    metadata: dict[str, str] | None,
    record_id: int | None,
) -> None:
    metadata = metadata or {}
    action_url = url_for("expenseai_auth.verify_otp", purpose=purpose, email=user.email, _external=True)
    subject, _ = _otp_labels(purpose)
    context = _otp_email_context(user, otp_code, purpose, action_url, metadata)
    send_email(
        subject=subject,
        recipients=[user.email],
        html_template="emails/otp.html",
        text_template="emails/otp.txt",
        context=context,
    )
    current_app.logger.info(
        "OTP email sent",
        extra={"user_id": user.id, "purpose": purpose, "otp_id": record_id},
    )


def _send_otp_email(user, *, purpose: str, metadata: dict[str, str] | None = None) -> None:
    """Issue an OTP and dispatch the transactional email."""
    metadata = metadata or {}
    otp_code, otp_record = otp_service.issue_otp(user, purpose=purpose, metadata=metadata)
    _dispatch_otp_email(user, otp_code=otp_code, purpose=purpose, metadata=metadata, record_id=otp_record.id)


def _send_confirmation_email(user, *, purpose: str, message: str) -> None:
    """Send a follow-up confirmation email."""
    headline, _ = _otp_labels(purpose)
    if purpose == OTP_PURPOSE_PASSWORD_RESET:
        subject = f"{current_app.config.get('APP_NAME', 'Finvela')} password updated"
        cta_label = "Sign in"
        cta_url = url_for("expenseai_auth.login", _external=True)
    else:
        subject = f"Welcome to {current_app.config.get('APP_NAME', 'Finvela')}"
        cta_label = "Go to dashboard"
        cta_url = url_for("expenseai_auth.login", _external=True)
    context = {
        "app_name": current_app.config.get("APP_NAME", "Finvela"),
        "name": user.full_name,
        "headline": headline,
        "message": message,
        "cta_label": cta_label,
        "cta_url": cta_url,
        "support_email": current_app.config.get("EMAIL_FROM"),
    }
    send_email(
        subject=subject,
        recipients=[user.email],
        html_template="emails/confirmation.html",
        text_template="emails/confirmation.txt",
        context=context,
    )


def _store_password_reset_session(user: User) -> None:
    """Persist password reset permission in the session."""
    ttl_minutes = max(int(current_app.config.get("OTP_EXPIRY_MINUTES", 10)), 5)
    expires_at = datetime.utcnow() + timedelta(minutes=ttl_minutes)
    session[PASSWORD_RESET_SESSION_KEY] = {
        "user_id": user.id,
        "expires_at": expires_at.isoformat(),
    }
    session.modified = True


def _load_password_reset_session() -> tuple[User | None, bool]:
    """Retrieve the pending password reset session, if any."""
    state = session.get(PASSWORD_RESET_SESSION_KEY)
    if not state:
        return None, False
    try:
        expires_at = datetime.fromisoformat(state.get("expires_at", ""))
    except (TypeError, ValueError):
        session.pop(PASSWORD_RESET_SESSION_KEY, None)
        return None, False
    if expires_at < datetime.utcnow():
        session.pop(PASSWORD_RESET_SESSION_KEY, None)
        return None, True
    user = db.session.get(User, state.get("user_id")) if state.get("user_id") else None
    if not user:
        session.pop(PASSWORD_RESET_SESSION_KEY, None)
        return None, True
    return user, False


def _clear_password_reset_session() -> None:
    session.pop(PASSWORD_RESET_SESSION_KEY, None)
    session.modified = True


def _attempt_otp_verification(user: User | None, *, purpose: str, code: str) -> tuple[str, dict[str, str] | None, str]:
    """Try verifying an OTP code and return (status, metadata, message)."""
    if not user:
        return "error", None, "Invalid or expired verification code."
    try:
        metadata = otp_service.verify_otp(user, purpose=purpose, candidate=code)
        return "success", metadata, ""
    except OtpExpiredError:
        return "error", None, "This code has expired. Request a new one."
    except OtpNotFoundError:
        return "error", None, "Verification code not found. Request a new one."
    except OtpValidationError:
        return "error", None, "That code is incorrect. Please try again."
    except OtpAttemptsExceededError:
        return "locked", None, "Maximum attempts reached. Request a new verification code."


def _handle_password_reset_request(email: str, *, respond_json: bool = False):
    """Generate a password reset OTP while avoiding account enumeration."""
    normalized = email.strip().lower()
    generic_message = "If the account exists we sent a reset code."
    user = UserService.get_by_email(normalized)
    if user:
        _send_otp_email(user, purpose=OTP_PURPOSE_PASSWORD_RESET, metadata={})
        AuditLog.log(action="password_reset_requested", entity="user", entity_id=user.id, data={})
    if respond_json:
        return {"status": "ok", "message": generic_message}
    flash(generic_message, "info")
    return redirect(url_for("expenseai_auth.verify_otp", purpose=OTP_PURPOSE_PASSWORD_RESET, email=normalized))


def _handle_otp_success(user: User, *, purpose: str, metadata: dict[str, str] | None, respond_json: bool = False):
    """Finalize OTP success for registration or password reset."""
    metadata = metadata or {}
    now = datetime.utcnow()
    redirect_url = url_for("expenseai_auth.login")
    flash_category = "success"
    message = "Verification complete."
    confirmation_message: str | None = None

    if purpose == OTP_PURPOSE_REGISTRATION:
        account_type = (metadata.get("account_type") or "").lower()
        user.email_verified_at = now
        if account_type == "admin":
            user.is_active = True
            user.approved_at = now
            message = "Email verified. You can now sign in."
            flash_category = "success"
            confirmation_message = "Your admin account is confirmed. Sign in to continue."
        else:
            message = "Email verified. Await administrator approval before signing in."
            flash_category = "info"
            confirmation_message = "Your email is verified. An administrator will activate your account soon."
        redirect_url = url_for("expenseai_auth.login")
        AuditLog.log(
            action="registration_verified",
            entity="user",
            entity_id=user.id,
            data={"account_type": account_type or "unknown"},
        )
    else:
        if not user.email_verified_at:
            user.email_verified_at = now
        _store_password_reset_session(user)
        message = "Code verified. Choose a new password."
        flash_category = "success"
        redirect_url = url_for("expenseai_auth.reset_password")
        AuditLog.log(
            action="password_reset_code_verified",
            entity="user",
            entity_id=user.id,
            data={},
        )

    db.session.add(user)
    db.session.commit()

    if confirmation_message and purpose == OTP_PURPOSE_REGISTRATION:
        _send_confirmation_email(user, purpose=purpose, message=confirmation_message)

    if respond_json:
        return {
            "status": "ok",
            "message": message,
            "redirect": redirect_url,
        }

    flash(message, flash_category)
    return redirect(redirect_url)


@auth_bp.route("/login", methods=["GET", "POST"])
@limiter.limit(lambda: current_app.config["RATES"]["LOGIN"], key_func=user_or_ip_rate_limit())
def login():
    """Log an existing user into the system."""
    if current_user.is_authenticated:
        return redirect(url_for("expenseai_web.dashboard"))
    form = LoginForm()
    if form.validate_on_submit():
        user = UserService.get_by_email(form.email.data)
        if not user or not user.verify_password(form.password.data):
            flash("Invalid credentials", "danger")
            AuditLog.log(action="login_failed", entity="user", entity_id=None, data={"email": form.email.data})
            return render_template("auth/login.html", form=form)
        if not user.is_active:
            if user.organization_id and not user.approved_at:
                flash("Your account is awaiting approval from your organization admin.", "warning")
            else:
                flash("Account is inactive. Contact an administrator.", "warning")
            AuditLog.log(action="login_denied_inactive", entity="user", entity_id=user.id)
            return render_template("auth/login.html", form=form)
        login_user(user, remember=form.remember_me.data)
        _create_session_record(user, remember=form.remember_me.data)
        AuditLog.log(action="login", entity="user", entity_id=user.id)
        flash("Welcome back!", "success")
        return redirect(request.args.get("next") or url_for("expenseai_web.dashboard"))
    return render_template("auth/login.html", form=form)


@auth_bp.route("/register", methods=["GET", "POST"])
@limiter.limit(lambda: current_app.config["RATES"]["REGISTER"], key_func=user_or_ip_rate_limit())
def register():
    """Register a new user when permitted by configuration."""
    allow_admin_signup = current_app.config.get("ALLOW_SELF_REGISTRATION", False)
    allow_member_signup = current_app.config.get("ALLOW_INVITE_REGISTRATION", True)
    if not allow_admin_signup and not allow_member_signup:
        abort(404)
    if current_user.is_authenticated:
        return redirect(url_for("expenseai_web.dashboard"))
    form = RegisterForm()
    account_choices: list[tuple[str, str]] = []
    if allow_admin_signup:
        account_choices.append(("admin", "An admin creating a new organization"))
    if allow_member_signup:
        account_choices.append(("member", "A member joining an existing organization"))
    form.account_type.choices = account_choices
    valid_choice_values = {value for value, _ in account_choices}

    raw_account_type = None
    if form.is_submitted():
        raw_account_type = request.form.get(form.account_type.name)
    if raw_account_type in valid_choice_values:
        form.account_type.data = raw_account_type
    elif form.account_type.data not in valid_choice_values and account_choices:
        form.account_type.data = account_choices[0][0]

    selected_account_type = (
        form.account_type.data if form.account_type.data in valid_choice_values else (account_choices[0][0] if account_choices else None)
    )

    if form.validate_on_submit():
        account_type = form.account_type.data
        if account_type == "admin":
            if not allow_admin_signup:
                abort(403)
            organization_name = (form.organization_name.data or "").strip()
            if not organization_name:
                form.organization_name.errors.append("Organization name is required for admin registration.")
                return render_template(
                    "auth/register.html",
                    form=form,
                    requires_invite=False,
                    selected_account_type=account_type,
                    allow_admin_signup=allow_admin_signup,
                    allow_member_signup=allow_member_signup,
                )
            try:
                user = UserService.create_user(
                    form.full_name.data,
                    form.email.data,
                    form.password.data,
                    roles=["admin", "viewer"],
                    is_active=False,
                    approved_at=None,
                )
            except ValueError as exc:
                flash(str(exc), "danger")
                return render_template(
                    "auth/register.html",
                    form=form,
                    requires_invite=False,
                    selected_account_type=account_type,
                    allow_admin_signup=allow_admin_signup,
                    allow_member_signup=allow_member_signup,
                )
            try:
                organization = OrganizationService.create_organization(organization_name, user, activate_admin=False)
            except ValueError as exc:
                db.session.delete(user)
                db.session.commit()
                flash(str(exc), "danger")
                return render_template(
                    "auth/register.html",
                    form=form,
                    requires_invite=False,
                    selected_account_type=account_type,
                    allow_admin_signup=allow_admin_signup,
                    allow_member_signup=allow_member_signup,
                )
            AuditLog.log(
                action="register_admin",
                entity="user",
                entity_id=user.id,
                data={"organization_id": organization.id},
            )
            _send_otp_email(
                user,
                purpose=OTP_PURPOSE_REGISTRATION,
                metadata={
                    "account_type": "admin",
                    "organization_id": str(organization.id),
                },
            )
            flash(
                f"We emailed a verification code to {_mask_email(user.email)}. Enter it below to activate your account.",
                "info",
            )
            return redirect(
                url_for(
                    "expenseai_auth.verify_otp",
                    purpose=OTP_PURPOSE_REGISTRATION,
                    email=user.email,
                )
            )

        if account_type == "member":
            if not allow_member_signup:
                abort(403)
            invite_code = (form.invite_code.data or "").strip()
            invite = None
            if invite_code:
                invite = OrganizationService.validate_invite(invite_code)
                if not invite:
                    form.invite_code.errors.append("Invitation code is invalid or has expired.")
                    return render_template(
                        "auth/register.html",
                        form=form,
                        requires_invite=True,
                        selected_account_type=account_type,
                        allow_admin_signup=allow_admin_signup,
                        allow_member_signup=allow_member_signup,
                    )
            else:
                form.invite_code.errors.append("Invitation code is required to join your organization.")
                return render_template(
                    "auth/register.html",
                    form=form,
                    requires_invite=True,
                    selected_account_type=account_type,
                    allow_admin_signup=allow_admin_signup,
                    allow_member_signup=allow_member_signup,
                )

            organization = invite.organization if invite else None
            if organization and not OrganizationService.can_add_members(organization):
                flash(OrganizationService.LIMIT_REACHED_MESSAGE, "warning")
                return render_template(
                    "auth/register.html",
                    form=form,
                    requires_invite=True,
                    selected_account_type=account_type,
                    allow_admin_signup=allow_admin_signup,
                    allow_member_signup=allow_member_signup,
                )
            try:
                user = UserService.create_user(
                    form.full_name.data,
                    form.email.data,
                    form.password.data,
                    roles=["viewer"],
                    organization=organization,
                    is_active=False,
                    approved_at=None,
                )
            except ValueError as exc:
                flash(str(exc), "danger")
                return render_template(
                    "auth/register.html",
                    form=form,
                    requires_invite=True,
                    selected_account_type=account_type,
                    allow_admin_signup=allow_admin_signup,
                    allow_member_signup=allow_member_signup,
                )

            if invite:
                OrganizationService.consume_invite(invite)
            AuditLog.log(
                action="register_invited",
                entity="user",
                entity_id=user.id,
                data={"organization_id": organization.id if organization else None},
            )
            _send_otp_email(
                user,
                purpose=OTP_PURPOSE_REGISTRATION,
                metadata={
                    "account_type": "member",
                    "organization_id": str(organization.id) if organization else "",
                },
            )
            flash(
                f"We emailed a verification code to {_mask_email(user.email)}. Verify your address to finish registration.",
                "info",
            )
            return redirect(
                url_for(
                    "expenseai_auth.verify_otp",
                    purpose=OTP_PURPOSE_REGISTRATION,
                    email=user.email,
                )
            )

        flash("Please choose a valid registration option.", "danger")

    requires_invite = selected_account_type == "member"
    return render_template(
        "auth/register.html",
        form=form,
        requires_invite=requires_invite,
        selected_account_type=selected_account_type,
        allow_admin_signup=allow_admin_signup,
        allow_member_signup=allow_member_signup,
    )


@auth_bp.route("/verify-otp", methods=["GET", "POST"])
@limiter.limit(lambda: current_app.config["RATES"]["OTP_VERIFY"], key_func=user_or_ip_rate_limit())
def verify_otp():
    """Render and process OTP verification for registration and password reset."""
    purpose_arg = (request.args.get("purpose") or OTP_PURPOSE_REGISTRATION).strip() or OTP_PURPOSE_REGISTRATION
    email_arg = (request.args.get("email") or "").strip()

    # JSON requests are treated as API calls.
    if request.method == "POST" and request.is_json:
        payload = request.get_json(silent=True) or {}
        email = (payload.get("email") or "").strip().lower()
        purpose = (payload.get("purpose") or purpose_arg).strip() or OTP_PURPOSE_REGISTRATION
        code = (payload.get("otp") or "").strip()
        user = UserService.get_by_email(email)
        status, metadata, message = _attempt_otp_verification(user, purpose=purpose, code=code)
        if status == "success" and user is not None:
            response = _handle_otp_success(user, purpose=purpose, metadata=metadata or {}, respond_json=True)
            return response, 200
        if status == "locked":
            return {"status": "locked", "message": message}, 423
        return {"status": "error", "message": message}, 400

    form = OtpVerificationForm()
    if request.method == "GET":
        form.purpose.data = purpose_arg
        form.email.data = email_arg

    if not form.purpose.data:
        form.purpose.data = purpose_arg
    if not form.email.data:
        form.email.data = email_arg

    attempts_remaining = None
    resend_cooldown = 0
    throttle_seconds = int(current_app.config.get("RESEND_THROTTLE_SECONDS", 60))
    active_user = None
    if form.email.data:
        active_user = UserService.get_by_email(form.email.data.lower().strip())
        if active_user:
            try:
                otp_record = otp_service.get_active_otp(active_user, purpose=form.purpose.data)
            except OtpExpiredError:
                otp_record = None
                flash("The previous code expired. Request a new one.", "warning")
            except OtpError:
                otp_record = None
            else:
                attempts_remaining = otp_record.attempts_remaining
                last_sent = otp_record.updated_at or otp_record.created_at
                delta = datetime.utcnow() - last_sent
                resend_cooldown = max(0, throttle_seconds - int(delta.total_seconds()))

    if form.validate_on_submit():
        email = form.email.data.strip().lower()
        purpose = form.purpose.data.strip() or OTP_PURPOSE_REGISTRATION
        code = (form.otp.data or "").strip()
        user = UserService.get_by_email(email)
        status, metadata, message = _attempt_otp_verification(user, purpose=purpose, code=code)
        if status == "success" and user is not None:
            return _handle_otp_success(user, purpose=purpose, metadata=metadata or {})
        if status == "locked":
            flash(message, "danger")
            return redirect(url_for("expenseai_auth.verify_otp", purpose=purpose, email=email))
        form.otp.errors.append(message)

    masked_email = _mask_email(form.email.data) if form.email.data else None
    headline, cta_label = _otp_labels(form.purpose.data)
    expiry_minutes = int(current_app.config.get("OTP_EXPIRY_MINUTES", 10))
    return render_template(
        "auth/verify_otp.html",
        form=form,
        masked_email=masked_email,
        attempts_remaining=attempts_remaining,
        resend_cooldown=resend_cooldown,
        resend_throttle=throttle_seconds,
        purpose_label=headline,
        cta_label=cta_label,
        expiry_minutes=expiry_minutes,
    )


@auth_bp.route("/resend-otp", methods=["POST"])
@limiter.limit(lambda: current_app.config["RATES"]["OTP_SEND"], key_func=user_or_ip_rate_limit())
def resend_otp():
    """Resend an OTP for the given email and purpose."""
    respond_json = request.is_json or request.headers.get("Accept") == "application/json"
    payload = request.get_json(silent=True) or request.form
    email = (payload.get("email") or "").strip().lower()
    purpose = (payload.get("purpose") or OTP_PURPOSE_REGISTRATION).strip() or OTP_PURPOSE_REGISTRATION

    redirect_target = url_for("expenseai_auth.verify_otp", purpose=purpose, email=email)
    generic_message = "If the account exists we sent a new code."

    if not email:
        if respond_json:
            return {"status": "error", "message": "Email is required."}, 400
        flash("Provide the email associated with your account.", "danger")
        return redirect(redirect_target)

    user = UserService.get_by_email(email)
    if not user:
        if respond_json:
            return {"status": "ok", "message": generic_message}, 200
        flash(generic_message, "info")
        return redirect(redirect_target)

    metadata: dict[str, str] = {}
    otp_code: str
    otp_id: int | None = None

    try:
        record = otp_service.get_active_otp(user, purpose=purpose)
    except OtpExpiredError:
        record = None
    except OtpError:
        record = None

    try:
        if record is not None:
            metadata = record.metadata_json or {}
            otp_code, new_record = otp_service.resend_otp(record)
            otp_id = new_record.id
            metadata = new_record.metadata_json or metadata
        else:
            if purpose == OTP_PURPOSE_REGISTRATION:
                metadata = {
                    "account_type": "admin" if user.has_role("admin") else "member",
                }
                if user.organization_id:
                    metadata["organization_id"] = str(user.organization_id)
            otp_code, new_record = otp_service.issue_otp(user, purpose=purpose, metadata=metadata)
            otp_id = new_record.id
            metadata = new_record.metadata_json or metadata
    except OtpThrottleError:
        message = "Please wait before requesting another code."
        if respond_json:
            return {"status": "throttled", "message": message}, 429
        flash(message, "warning")
        return redirect(redirect_target)

    _dispatch_otp_email(user, otp_code=otp_code, purpose=purpose, metadata=metadata, record_id=otp_id)

    message = f"We sent a new code to {_mask_email(user.email)}."
    if respond_json:
        return {
            "status": "ok",
            "message": message,
            "cooldown": int(current_app.config.get("RESEND_THROTTLE_SECONDS", 60)),
        }, 200

    flash(message, "success")
    return redirect(redirect_target)


@auth_bp.route("/forgot-password", methods=["GET", "POST"])
@limiter.limit(lambda: current_app.config["RATES"]["PASSWORD_RESET"], key_func=user_or_ip_rate_limit())
def forgot_password():
    """Initiate a password reset by sending an OTP."""
    if current_user.is_authenticated:
        return redirect(url_for("expenseai_web.dashboard"))
    form = ForgotPasswordForm()
    if form.validate_on_submit():
        return _handle_password_reset_request(form.email.data)
    return render_template("auth/forgot_password.html", form=form)


@auth_bp.route("/request-reset", methods=["POST"])
@limiter.limit(lambda: current_app.config["RATES"]["PASSWORD_RESET"], key_func=user_or_ip_rate_limit())
def request_reset_api():
    """API endpoint to initiate password reset via OTP."""
    respond_json = request.is_json or request.headers.get("Accept") == "application/json"
    payload = request.get_json(silent=True) or request.form
    email = (payload.get("email") or "").strip()
    if not email:
        if respond_json:
            return {"status": "error", "message": "Email is required."}, 400
        flash("Enter the email associated with your account.", "danger")
        return redirect(url_for("expenseai_auth.forgot_password"))
    result = _handle_password_reset_request(email, respond_json=respond_json)
    if respond_json:
        return result, 200
    return result


@auth_bp.route("/verify-reset-otp", methods=["POST"])
@limiter.limit(lambda: current_app.config["RATES"]["OTP_VERIFY"], key_func=user_or_ip_rate_limit())
def verify_reset_otp_api():
    """API endpoint dedicated to password reset OTP verification."""
    respond_json = request.is_json or request.headers.get("Accept") == "application/json"
    payload = request.get_json(silent=True) or request.form
    email = (payload.get("email") or "").strip().lower()
    code = (payload.get("otp") or "").strip()
    if not email or not code:
        message = "Email and OTP are required."
        if respond_json:
            return {"status": "error", "message": message}, 400
        flash(message, "danger")
        return redirect(url_for("expenseai_auth.verify_otp", purpose=OTP_PURPOSE_PASSWORD_RESET, email=email))
    user = UserService.get_by_email(email)
    status, metadata, message = _attempt_otp_verification(user, purpose=OTP_PURPOSE_PASSWORD_RESET, code=code)
    if status == "success" and user is not None:
        return _handle_otp_success(user, purpose=OTP_PURPOSE_PASSWORD_RESET, metadata=metadata or {}, respond_json=respond_json)
    status_code = 423 if status == "locked" else 400
    if respond_json:
        state = "locked" if status == "locked" else "error"
        return {"status": state, "message": message}, status_code
    flash(message, "danger")
    return redirect(url_for("expenseai_auth.verify_otp", purpose=OTP_PURPOSE_PASSWORD_RESET, email=email))


@auth_bp.route("/reset-password", methods=["GET", "POST"])
@limiter.limit(lambda: current_app.config["RATES"]["PASSWORD_RESET"], key_func=user_or_ip_rate_limit())
def reset_password():
    """Allow a user with a verified OTP to set a new password."""
    user, expired = _load_password_reset_session()
    if not user:
        if expired:
            flash("Your reset session expired. Request a new code.", "warning")
        else:
            flash("Request a reset code before setting a new password.", "info")
        return redirect(url_for("expenseai_auth.forgot_password"))

    form = ResetPasswordForm()
    if form.validate_on_submit():
        user.set_password(form.password.data)
        if not user.email_verified_at:
            user.email_verified_at = datetime.utcnow()
        db.session.add(user)
        db.session.commit()
        AuditLog.log(action="password_reset_completed", entity="user", entity_id=user.id, data={})
        _clear_password_reset_session()
        flash("Password updated. You can now sign in.", "success")
        _send_confirmation_email(user, purpose=OTP_PURPOSE_PASSWORD_RESET, message="Your password was updated successfully.")
        return redirect(url_for("expenseai_auth.login"))

    return render_template(
        "auth/reset_password.html",
        form=form,
        masked_email=_mask_email(user.email),
    )


@auth_bp.route("/logout", methods=["POST"])
@login_required
def logout():
    """Terminate the current user session."""
    AuditLog.log(action="logout", entity="user", entity_id=current_user.id)
    logout_user()
    flash("You have been signed out.", "info")
    return redirect(url_for("expenseai_web.index"))
