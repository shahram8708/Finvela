"""Forms for compliance admin workflows."""
from __future__ import annotations

from flask_wtf import FlaskForm
from flask_wtf.file import FileAllowed, FileField, FileRequired
from wtforms import BooleanField


class HsnUploadForm(FlaskForm):
    """Upload form for GST HSN/SAC CSV data."""

    file = FileField(
        "CSV File",
        validators=[
            FileRequired(message="Select a CSV file to upload."),
            FileAllowed(["csv"], "Upload a CSV file."),
        ],
    )
    replace_existing = BooleanField("Replace existing rates", default=True)
