"""Forms for authentication workflows."""
from __future__ import annotations

from flask_wtf import FlaskForm
from wtforms import BooleanField, EmailField, HiddenField, PasswordField, RadioField, StringField, SubmitField
from wtforms.validators import DataRequired, Email, EqualTo, Length, Optional, Regexp


class LoginForm(FlaskForm):
    """Authenticate an existing user."""

    email = EmailField("Email", validators=[DataRequired(), Email()])
    password = PasswordField("Password", validators=[DataRequired()])
    remember_me = BooleanField("Remember me")
    submit = SubmitField("Sign in")


class RegisterForm(FlaskForm):
    """Create a new user account when self-registration is enabled."""

    account_type = RadioField(
        "I'm registering as",
        choices=[
            ("admin", "An admin creating a new organization"),
            ("member", "A member joining an existing organization"),
        ],
        default="admin",
        validators=[DataRequired()],
    )
    full_name = StringField("Full name", validators=[DataRequired(), Length(min=2, max=120)])
    email = EmailField("Email", validators=[DataRequired(), Email()])
    password = PasswordField(
        "Password",
        validators=[
            DataRequired(),
            Length(min=8, message="Password must be at least 8 characters long."),
        ],
    )
    confirm_password = PasswordField(
        "Confirm password",
        validators=[DataRequired(), EqualTo("password", message="Passwords must match.")],
    )
    organization_name = StringField(
        "Organization name",
        validators=[Optional(), Length(min=3, max=120)],
        render_kw={"data-required": "true"},
    )
    invite_code = StringField(
        "Invitation code",
        validators=[Optional(), Length(min=6, max=64, message="Enter a valid invitation code.")],
        render_kw={"data-required": "true"},
    )
    submit = SubmitField("Create account")


class OtpVerificationForm(FlaskForm):
    """Verify an email OTP for registration or password reset."""

    email = EmailField("Email", validators=[DataRequired(), Email()])
    purpose = HiddenField(validators=[DataRequired(), Length(max=32)])
    otp = StringField(
        "Verification code",
        validators=[
            DataRequired(),
            Length(min=6, max=6, message="Enter the 6-digit code."),
            Regexp(r"^\d{6}$", message="Codes contain exactly six digits."),
        ],
    )
    submit = SubmitField("Verify code")


class ForgotPasswordForm(FlaskForm):
    """Request a password reset OTP."""

    email = EmailField("Email", validators=[DataRequired(), Email()])
    submit = SubmitField("Send reset code")


class ResetPasswordForm(FlaskForm):
    """Reset password after OTP verification."""

    password = PasswordField(
        "New password",
        validators=[
            DataRequired(),
            Length(min=8, message="Password must be at least 8 characters long."),
        ],
    )
    confirm_password = PasswordField(
        "Confirm password",
        validators=[DataRequired(), EqualTo("password", message="Passwords must match.")],
    )
    submit = SubmitField("Update password")
