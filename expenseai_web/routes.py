"""Routes for the public and authenticated areas of the site."""
from __future__ import annotations

import json
import time
from datetime import datetime

from expenseai_models import ContactMessage
from flask import Response, abort, current_app, flash, jsonify, redirect, render_template, request, session, stream_with_context, url_for
from flask_login import current_user, login_required
from sqlalchemy import text

from expenseai_ai import gemini_client
from expenseai_auth.billing import (
    BillingConfigurationError,
    BillingError,
    OrganizationBillingService,
    PaymentVerificationError,
    PricingBreakdown,
)
from expenseai_auth.services import OrganizationService
from expenseai_ext import auth as auth_ext
from expenseai_ext.db import db
from expenseai_ext.email import send_email
from expenseai_invoices.forms import InvoiceUploadForm
from expenseai_models.invoice import INVOICE_STATUSES, Invoice
from expenseai_models.invoice_event import InvoiceEvent
from expenseai_web import web_bp
from expenseai_web.forms import ContactForm, OrganizationUpgradeForm, PaymentConfirmationForm, TeamInviteForm


@web_bp.route("/")
def index() -> str:
    """Landing page that redirects authenticated users to the dashboard."""
    if current_user.is_authenticated:
        return redirect(url_for("expenseai_web.dashboard"))
    return render_template("dashboard/index.html", is_landing=True)


@web_bp.route("/privacy")
def privacy_policy() -> str:
    """Render the public privacy policy page."""
    return render_template("pages/privacy.html")


@web_bp.route("/terms")
def terms_and_conditions() -> str:
    """Render the public terms and conditions page."""
    app_name = current_app.config.get("APP_NAME", "Finvela")
    return render_template("pages/terms.html", app_name=app_name)


@web_bp.route("/about")
def about() -> str:
    """Render the public about page with company story and team highlights."""
    team_members = [
        {"name": "Shah Ram", "role": "Full Stack Developer", "initials": "SR"},
        {"name": "Nisarg Parmar", "role": "Backend Developer", "initials": "NP"},
        {"name": "Milan Gohil", "role": "AI/ML Developer", "initials": "MG"},
        {"name": "Mahir Sanghavi", "role": "Frontend Developer", "initials": "MS"},
    ]
    milestones = [
        {
            "year": "2025",
            "title": "Finvela is born",
            "description": "We set out to remove friction from expense audits with a privacy-first, AI-native platform.",
        },
        {
            "year": "2025",
            "title": "First enterprise rollout",
            "description": "Scaled to thousands of invoices per week while maintaining 99.9% data accuracy.",
        },
        {
            "year": "2025",
            "title": "Global expansion",
            "description": "Opened regional hubs across APAC, EMEA, and North America to support hybrid teams.",
        },
        {
            "year": "2025",
            "title": "Responsible AI milestone",
            "description": "Launched explainable risk scoring and privacy controls that set a new industry benchmark.",
        },
    ]
    return render_template("pages/about.html", team_members=team_members, milestones=milestones)


@web_bp.route("/contact", methods=["GET"])
def contact() -> str:
    """Render the public contact page with support options."""
    form = ContactForm()
    return render_template("pages/contact.html", form=form, categories=ContactForm.CATEGORY_CHOICES)


@web_bp.route("/contact/submit", methods=["POST"])
def submit_contact() -> Response:
    """Handle contact form submissions and trigger notification emails."""

    form = ContactForm()
    if not form.validate_on_submit():
        return jsonify({"status": "error", "errors": form.errors}), 400

    admin_email = (current_app.config.get("ADMIN_EMAIL") or "").strip()
    if not admin_email:
        current_app.logger.error("Contact form submission failed: ADMIN_EMAIL is not configured")
        return jsonify({"status": "error", "message": "Contact routing is temporarily unavailable."}), 500

    category_label = dict(ContactForm.CATEGORY_CHOICES).get(form.category.data, form.category.data.title())

    submission = ContactMessage(
        name=form.full_name.data.strip(),
        email=form.email.data.strip(),
        subject=form.subject.data.strip(),
        category=category_label,
        message=form.message.data.strip(),
    )

    try:
        db.session.add(submission)
        db.session.commit()
    except Exception:  # pragma: no cover - defensive logging
        db.session.rollback()
        current_app.logger.exception("Failed to persist contact submission")
        return jsonify({"status": "error", "message": "We could not save your message. Please try again."}), 500

    context = {
        "submission": submission,
        "app_name": current_app.config.get("APP_NAME", "Finvela"),
    }

    subject = f"New Contact Form Submission – {category_label}"

    try:
        send_email(
            subject=subject,
            recipients=[admin_email],
            html_template="email/contact_notification.html",
            text_template="email/contact_notification.txt",
            context=context,
        )
    except Exception:  # pragma: no cover - email failure handling
        current_app.logger.exception("Failed to dispatch contact notification email")
        return jsonify({"status": "error", "message": "We saved your message but could not notify the team."}), 502

    return jsonify({
        "status": "success",
        "message": "Your message has been sent successfully.",
    }), 201

@web_bp.route("/set-locale/<locale>")
def set_locale(locale):
    session["preferred_locale"] = locale
    next_url = request.referrer or url_for("expenseai_web.index")
    return redirect(next_url)

@web_bp.route("/dashboard")
@login_required
def dashboard() -> str:
    """Render the authenticated dashboard with recent invoices and activity."""
    upload_form = InvoiceUploadForm()
    org_id = getattr(current_user, "organization_id", None)

    if org_id is None:
        recent_invoices: list[Invoice] = []
        recent_events: list[InvoiceEvent] = []
    else:
        recent_invoices = (
            Invoice.query.filter(Invoice.organization_id == org_id)
            .order_by(Invoice.created_at.desc())
            .limit(6)
            .all()
        )
        recent_events = (
            InvoiceEvent.query.join(Invoice)
            .filter(Invoice.organization_id == org_id)
            .order_by(InvoiceEvent.created_at.desc())
            .limit(12)
            .all()
        )
    last_invoice = recent_invoices[0] if recent_invoices else None
    return render_template(
        "dashboard/index.html",
        is_landing=False,
        upload_form=upload_form,
        recent_invoices=recent_invoices,
        recent_events=recent_events,
        last_invoice=last_invoice,
        statuses=INVOICE_STATUSES,
        max_upload_mb=current_app.config.get("MAX_UPLOAD_MB", 10),
    )


@web_bp.route("/admin")
@login_required
@auth_ext.roles_required("admin")
def admin_portal() -> str:
    """Admin-only placeholder route to validate RBAC."""
    return redirect(url_for("expenseai_compliance_admin.hsn_upload"))


@web_bp.route("/admin/members", methods=["GET", "POST"])
@login_required
@auth_ext.roles_required("admin")
def manage_members() -> str:
    """Allow admins to generate invites and approve organization members."""

    organization = current_user.organization
    if organization is None:
        flash("Your account is not yet linked to an organization. Contact support.", "danger")
        return redirect(url_for("expenseai_web.dashboard"))

    usage = OrganizationService.member_usage(organization)
    billing_enabled = OrganizationBillingService.is_configured()
    form_name = request.form.get("_form_name") if request.method == "POST" else None

    invite_form = TeamInviteForm(request.form if form_name == "invite" else None, prefix="invite")

    if request.method == "POST" and form_name == "invite":
        if usage.limit_reached:
            flash(OrganizationService.LIMIT_REACHED_MESSAGE, "warning")
            return redirect(url_for("expenseai_web.manage_members"))
        if invite_form.validate_on_submit():
            try:
                invite = OrganizationService.issue_invite(
                    current_user,
                    expires_in_hours=invite_form.expires_in_hours.data,
                    max_uses=invite_form.max_uses.data,
                )
            except ValueError as exc:
                flash(str(exc), "danger")
            else:
                flash(f"Invitation code {invite.code} created.", "success")
        else:
            flash("Please correct the errors in the invite form.", "danger")
        return redirect(url_for("expenseai_web.manage_members"))

    if request.method == "POST" and form_name == "approve":
        user_id_raw = request.form.get("user_id")
        if not user_id_raw:
            flash("Missing member identifier.", "danger")
            return redirect(url_for("expenseai_web.manage_members"))
        try:
            member_id = int(user_id_raw)
        except (TypeError, ValueError):
            flash("Invalid member identifier.", "danger")
            return redirect(url_for("expenseai_web.manage_members"))

        member = OrganizationService.get_member(organization, member_id)
        if not member:
            flash("User not found or not part of your organization.", "danger")
            return redirect(url_for("expenseai_web.manage_members"))
        try:
            OrganizationService.approve_member(current_user, member)
        except ValueError as exc:
            flash(str(exc), "danger")
        else:
            flash(f"{member.full_name} approved successfully.", "success")
        return redirect(url_for("expenseai_web.manage_members"))

    invites = OrganizationService.list_invites(organization)
    pending_users = OrganizationService.list_pending_members(organization)
    active_users = OrganizationService.list_active_members(organization)

    return render_template(
        "admin/members.html",
        organization=organization,
        invite_form=invite_form,
        invites=invites,
        pending_users=pending_users,
        active_users=active_users,
        usage=usage,
        billing_enabled=billing_enabled,
        limit_message=OrganizationService.LIMIT_REACHED_MESSAGE,
    )


@web_bp.route("/admin/billing", methods=["GET", "POST"])
@login_required
@auth_ext.roles_required("admin")
def manage_billing() -> str:
    """Allow organization admins to upgrade their seat limits via Razorpay checkout."""

    organization = current_user.organization
    if organization is None:
        flash("Your account is not yet linked to an organization. Contact support.", "danger")
        return redirect(url_for("expenseai_web.dashboard"))

    formdata = request.form if request.method == "POST" else None
    upgrade_form = OrganizationUpgradeForm(organization.user_limit, formdata=formdata, prefix="upgrade")
    if upgrade_form.desired_user_limit.data is None:
        upgrade_form.desired_user_limit.data = max(organization.user_limit + 5, organization.user_limit + 1)

    confirm_form = PaymentConfirmationForm(prefix="confirm")
    usage = OrganizationService.member_usage(organization)
    billing_enabled = OrganizationBillingService.is_configured()

    order_context: dict[str, object] | None = None
    if request.method == "POST":
        if not billing_enabled:
            flash("Payment gateway is not configured. Please contact support to enable upgrades.", "danger")
        elif upgrade_form.validate_on_submit():
            desired_limit = upgrade_form.desired_user_limit.data or organization.user_limit
            try:
                order_context = OrganizationBillingService.create_checkout_order(current_user, organization, desired_limit)
            except (BillingConfigurationError, BillingError) as exc:
                flash(str(exc), "danger")
            else:
                if order_context:
                    key_id = current_app.config.get("RAZORPAY_KEY_ID", "")
                    order_context["key_id"] = key_id
                    order_context["organization_name"] = organization.name
                    flash("Checkout initialized. Complete the payment to unlock additional seats.", "info")
        elif upgrade_form.errors:
            flash("Please correct the highlighted errors before continuing to checkout.", "danger")

    selected_limit = upgrade_form.desired_user_limit.data or organization.user_limit
    try:
        pricing_preview = OrganizationBillingService.build_pricing_breakdown(organization, selected_limit)
    except BillingConfigurationError as exc:
        current_app.logger.warning("Billing configuration error when building pricing preview", exc_info=exc)
        pricing_preview = PricingBreakdown(
            currency=OrganizationBillingService.get_currency(),
            current_limit=organization.user_limit,
            desired_limit=organization.user_limit,
            additional_users=0,
            per_user_price_minor=0,
            total_amount_minor=0,
        )

    billing_context = {
        "currency": pricing_preview.currency,
        "per_user_price_minor": pricing_preview.per_user_price_minor,
        "current_limit": pricing_preview.current_limit,
        "desired_limit": pricing_preview.desired_limit,
        "free_limit": OrganizationService.default_user_limit(),
        "member_count": usage.total,
        "active_members": usage.active,
        "pending_members": usage.pending,
        "remaining_slots": usage.remaining,
        "is_payment_configured": billing_enabled,
        "razorpay_key_id": current_app.config.get("RAZORPAY_KEY_ID", ""),
        "order": order_context,
    }

    transactions = organization.subscriptions
    transaction_rows = [
        {
            "created_at": tx.created_at,
            "order_id": tx.order_id,
            "payment_id": tx.payment_id,
            "additional_users": tx.additional_users,
            "purchased_user_limit": tx.purchased_user_limit,
            "amount_label": OrganizationBillingService.format_currency(tx.amount_minor, tx.currency),
        }
        for tx in transactions
    ]

    try:
        per_user_minor = OrganizationBillingService.get_per_user_price_minor()
    except BillingConfigurationError:
        per_user_minor = pricing_preview.per_user_price_minor
    per_user_price_label = OrganizationBillingService.format_currency(per_user_minor, pricing_preview.currency)
    total_amount_label = OrganizationBillingService.format_currency(pricing_preview.total_amount_minor, pricing_preview.currency)
    zero_amount_label = OrganizationBillingService.format_currency(0, pricing_preview.currency)

    return render_template(
        "admin/billing.html",
        organization=organization,
        usage=usage,
        upgrade_form=upgrade_form,
        confirm_form=confirm_form,
        pricing_preview=pricing_preview,
        billing_context=billing_context,
        transactions=transactions,
        per_user_price_label=per_user_price_label,
        transaction_rows=transaction_rows,
        total_amount_label=total_amount_label,
        zero_amount_label=zero_amount_label,
    )


@web_bp.route("/admin/billing/confirm", methods=["POST"])
@login_required
@auth_ext.roles_required("admin")
def confirm_billing() -> Response:
    """Handle Razorpay payment confirmations and persist the upgraded seat limit."""

    organization = current_user.organization
    if organization is None:
        flash("Your account is not yet linked to an organization. Contact support.", "danger")
        return redirect(url_for("expenseai_web.dashboard"))

    if not OrganizationBillingService.is_configured():
        flash("Payment gateway is not configured. Please contact support.", "danger")
        return redirect(url_for("expenseai_web.manage_billing"))

    form = PaymentConfirmationForm(prefix="confirm")
    current_app.logger.debug("Billing confirm payload: %s", request.form.to_dict(flat=False))
    if not form.validate_on_submit():
        flash("Payment confirmation data was incomplete. Please retry the checkout flow.", "danger")
        return redirect(url_for("expenseai_web.manage_billing"))

    try:
        desired_limit = int(form.desired_user_limit.data)
    except (TypeError, ValueError):
        flash("Unable to determine the requested seat limit from the payment confirmation.", "danger")
        return redirect(url_for("expenseai_web.manage_billing"))

    try:
        subscription = OrganizationBillingService.verify_and_record_payment(
            current_user,
            organization,
            order_id=form.razorpay_order_id.data,
            payment_id=form.razorpay_payment_id.data,
            signature=form.razorpay_signature.data,
            desired_limit=desired_limit,
        )
    except (PaymentVerificationError, BillingConfigurationError, BillingError) as exc:
        flash(str(exc), "danger")
        return redirect(url_for("expenseai_web.manage_billing"))

    flash(
        f"Payment confirmed. Your organization can now onboard up to {subscription.purchased_user_limit} users.",
        "success",
    )
    return redirect(url_for("expenseai_web.manage_billing"))


@web_bp.route("/health")
def health() -> Response:
    """Expose a simple health-check endpoint for orchestration systems."""
    db_ok = True
    try:
        db.session.execute(text("SELECT 1"))
    except Exception:  # pragma: no cover - defensive only
        current_app.logger.exception("Database health check failed")
        db_ok = False
    payload = {
        "app": current_app.config.get("APP_NAME", "expenseai"),
        "version": current_app.config.get("VERSION", "unknown"),
        "environment": current_app.config.get("ENV", "development"),
        "database": db_ok,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    return jsonify(payload)


@web_bp.route("/health/ai")
def health_ai() -> Response:
    """Return status about the Finvela API client plumbing."""
    status = gemini_client.healthcheck(current_app)
    return jsonify(status)


@web_bp.route("/events/stream")
@login_required
def events_stream() -> Response:
    """Stream invoice events to connected clients using SSE."""

    org_id = getattr(current_user, "organization_id", None)
    if org_id is None:
        abort(403)

    def generate(last_seen: int | None):
        current_last = last_seen or 0
        try:
            yield "retry: 3000\n\n"
            while True:
                events = (
                    InvoiceEvent.query.join(Invoice)
                    .filter(
                        InvoiceEvent.id > current_last,
                        Invoice.organization_id == org_id,
                    )
                    .order_by(InvoiceEvent.id.asc())
                    .limit(50)
                    .all()
                )
                if events:
                    for event in events:
                        current_last = event.id
                        payload = json.dumps(event.as_dict())
                        yield f"id: {event.id}\n"
                        yield "event: invoice\n"
                        yield f"data: {payload}\n\n"
                time.sleep(1)
        except GeneratorExit:  # pragma: no cover - connection closed
            current_app.logger.debug("SSE client disconnected")
        finally:
            db.session.remove()

    last_event_id = request.headers.get("Last-Event-ID")
    last_seen_int = None
    if last_event_id:
        try:
            last_seen_int = int(last_event_id)
        except ValueError:
            last_seen_int = None

    response = Response(stream_with_context(generate(last_seen_int)), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"
    return response


@web_bp.app_context_processor
def inject_template_globals() -> dict[str, object]:
    """Share metadata with templates such as app version and AI status."""
    ai_status = gemini_client.healthcheck(current_app)
    return {
        "app_version": current_app.config.get("VERSION", "dev"),
        "app_name": current_app.config.get("APP_NAME", "Finvela"),
        "ai_status": ai_status,
        "has_vendor_module": "expenseai_vendor" in current_app.blueprints,
        "now": datetime.utcnow,
    }
