"""Forms supporting invoice uploads and actions."""
from __future__ import annotations

from flask_wtf import FlaskForm
from wtforms import FileField, HiddenField, SelectField, StringField, SubmitField, TextAreaField
from wtforms.validators import DataRequired, Optional

from expenseai_models.invoice import INVOICE_STATUSES


class InvoiceUploadForm(FlaskForm):
    """Upload form exposed through drag-and-drop and classic input."""

    file = FileField("Invoice file", validators=[DataRequired()])
    submit = SubmitField("Upload")


class InvoiceActionForm(FlaskForm):
    """Handle actions taken on an invoice from the tri-panel UI."""

    action = HiddenField(validators=[DataRequired()])
    status = SelectField(
        "Set status",
        choices=[(status, status.title()) for status in INVOICE_STATUSES],
        validators=[Optional()],
    )
    assignee_id = SelectField("Assign to", coerce=int, validators=[Optional()])
    note = TextAreaField("Notes", validators=[Optional()])
    submit = SubmitField("Submit")
