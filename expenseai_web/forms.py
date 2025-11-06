"""Forms used by the public web blueprint."""
from __future__ import annotations

from flask_wtf import FlaskForm
from wtforms import HiddenField, IntegerField, SelectField, StringField, SubmitField, TextAreaField
from wtforms.validators import (
    DataRequired,
    Email,
    InputRequired,
    Length,
    NumberRange,
    Optional,
    ValidationError,
)


class EmptyForm(FlaskForm):
    """Simple form placeholder used for POST-only actions."""

    submit_token = HiddenField()


class TeamInviteForm(FlaskForm):
    """Generate invitation links for organization members."""

    expires_in_hours = IntegerField(
        "Expires in (hours)",
        validators=[Optional(), NumberRange(min=1, max=24 * 14)],
    )
    max_uses = IntegerField(
        "Maximum uses",
        validators=[Optional(), NumberRange(min=1, max=10000)],
    )
    submit = SubmitField("Generate invitation code")


class OrganizationUpgradeForm(FlaskForm):
    """Collects the desired organization-wide user limit before checkout."""

    desired_user_limit = IntegerField(
        "Total seats",
        validators=[InputRequired(), NumberRange(min=1, max=1000)],
        render_kw={"min": 1},
    )
    submit = SubmitField("Review payment")

    def __init__(self, current_limit: int, *args, **kwargs) -> None:
        self.current_limit = current_limit
        super().__init__(*args, **kwargs)
        self.desired_user_limit.render_kw = {**(self.desired_user_limit.render_kw or {}), "min": max(current_limit + 1, 1)}

    def validate_desired_user_limit(self, field: IntegerField) -> None:
        if field.data is None:
            raise ValidationError("Please select the total number of users you need.")
        if field.data <= self.current_limit:
            raise ValidationError("Select a total user count higher than your current limit to upgrade.")


class PaymentConfirmationForm(FlaskForm):
    """Holds Razorpay confirmation payload posted from the checkout handler."""

    razorpay_order_id = HiddenField(validators=[InputRequired()])
    razorpay_payment_id = HiddenField(validators=[InputRequired()])
    razorpay_signature = HiddenField(validators=[InputRequired()])
    desired_user_limit = HiddenField(validators=[InputRequired()])


class ContactForm(FlaskForm):
    """Public contact form with basic anti-spam protections."""

    CATEGORY_CHOICES = [
        ("general", "General Inquiry"),
        ("partnership", "Business Partnership"),
        ("feedback", "Feedback"),
        ("support", "Technical Support"),
        ("bug", "Report a Bug"),
        ("other", "Other"),
    ]

    full_name = StringField(
        "Full Name",
        validators=[DataRequired(message="Please add your name."), Length(max=255)],
        render_kw={"placeholder": "Your name"},
    )
    email = StringField(
        "Email Address",
        validators=[DataRequired(message="Email is required."), Email(), Length(max=255)],
        render_kw={"placeholder": "you@example.com"},
    )
    subject = StringField(
        "Subject",
        validators=[DataRequired(message="Add a short subject."), Length(min=3, max=255)],
        render_kw={"placeholder": "How can we help?"},
    )
    category = SelectField(
        "Category",
        choices=CATEGORY_CHOICES,
        validators=[DataRequired()],
    )
    message = TextAreaField(
        "Message",
        validators=[DataRequired(message="Tell us a bit more."), Length(min=10, max=5000)],
        render_kw={"rows": 6, "placeholder": "Share details so we can help quickly."},
    )
    honeypot = StringField(render_kw={"autocomplete": "off"})
    submit = SubmitField("Send Message")

    def validate_honeypot(self, field: StringField) -> None:
        if field.data:
            raise ValidationError("Spam detected.")


