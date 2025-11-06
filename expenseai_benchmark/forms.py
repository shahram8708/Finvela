"""Forms supporting benchmark administration."""
from __future__ import annotations

from flask_wtf import FlaskForm
from flask_wtf.file import FileAllowed, FileField, FileRequired


class BenchmarkUploadForm(FlaskForm):
    file = FileField(
        "CSV File",
        validators=[
            FileRequired(message="Select a CSV file to upload."),
            FileAllowed(["csv"], "Upload a CSV file."),
        ],
    )
