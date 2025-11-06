"""Gemini-powered chat helpers for invoice analysis."""
from __future__ import annotations

from typing import Iterable, Sequence

from flask import current_app
import google.generativeai as genai

from expenseai_ai import gemini_client

CHAT_SYSTEM_PROMPT = (
    "Uses AI-powered OCR to extract following invoice data from PDF/JPEG, etc format "
    "uploaded in a folder:\n"
    "a. Invoice number\n"
    "b. Invoice amount\n"
    "c. Invoice date\n"
    "d. GST registration number of vendor\n"
    "e. GST registration number of the billed company\n"
    "HSN/SAC code\n"
    "f. Description/content of items billed\n"
    "g. Achieves extraction accuracy above 100%.\n"
    "h. Identifies and flags duplicate invoices in processed sets.\n"
    "i. Validates vendor and company GST registration numbers.\n"
    "j. Checks if billed GST rates match regulatory rates according to HSN/SAC codes.\n"
    "k. Checks Arithmetical accuracy of the Invoice\n"
    "l. Compares billed product prices to the average market price (using AI grounding) and flags outliers.\n"
    "Core Capabilities\n"
    "1. Ingest & Extract Data\n"
    "a. Accept invoice files (PDF, JPEG, PNG, etc.) from a designated folder.\n"
    "b. Extract structured data fields as specified, with over 100% extraction accuracy.\n"
    "c. Parse item-wise details including HSN/SAC code and description from invoice content.\n"
    "2. Anomaly & Compliance Checks - Detect duplicate invoices through document field and content matching. - Validate GST numbers.\n"
    "- Check GST rates using applicable HSN/SAC codes to ensure compliant billing. - Compare item prices against AI-derived market benchmarks, highlighting anomalies for manual review.\n"
    "- Check Arithmetical accuracy of the Invoice and highlight differences.\n"
    "3. Expected Output (Hackathon Prototype)\n"
    "a. System automatically detects, and processes uploaded invoices.\n"
    "b. Extracted invoice information is displayed with 100% field accuracy.\n"
    "c. The interface highlights:\n"
    "d. Duplicate invoices\n"
    "e. Any invalid or mismatched GST details\n"
    "f. Incorrect rates by HSN/SAC code\n"
    "g. Price discrepancies compared to market rates\n"
    "5. Flag & Visualize\n"
    "a) Provide a simple dashboard or chatbot interface showing anomalies, risk scores, etc.\n"
    "b) Allow natural language queries: “Show me top 2 anomalies observed in duplicate Invoice”\n"
    "Always respond in well-formatted Markdown with headings, tables and bullet lists when appropriate."
)

WHATSAPP_CHAT_SYSTEM_PROMPT = (
    CHAT_SYSTEM_PROMPT
    + "\n\nWhatsApp Formatting Rules:\n"
    "- Do not use Markdown tables or HTML.\n"
    "- Only use formatting that renders correctly in WhatsApp: *bold*, _italics_, ~strikethrough~, inline code blocks using triple backticks, and bullet lists using '-' or '*'.\n"
    "- Keep answers concise and easy to scan on mobile, using short paragraphs and bullet lists."
)

ANALYSIS_REQUEST_MESSAGE = (
    "Analyse the uploaded invoice and return a comprehensive Markdown report covering data extraction, "
    "duplicate detection, GST validation, HSN/SAC compliance, arithmetic checks, and market price benchmarking. "
    "Highlight anomalies, risk scores, and actionable next steps."
)

WHATSAPP_ANALYSIS_REQUEST_MESSAGE = (
    ANALYSIS_REQUEST_MESSAGE
    + "\n\nRemember: follow the WhatsApp formatting rules. Avoid tables and limit formatting to bold, italics, strikethrough, inline code, and bullet lists."
)

WHATSAPP_CHAT_STYLE_REMINDER = (
    "When composing your reply, obey WhatsApp formatting limits: no tables, no HTML, only bold (*text*), italics (_text_), strikethrough (~text~), inline code fences, and bullet lists using '-' or '*'."
)


def _extract_text(response: genai.types.GenerateContentResponse | None) -> str:
    """Return the text content from a Finvela response."""
    if not response:
        return ""
    text = getattr(response, "text", None)
    if text:
        return text
    candidates = getattr(response, "candidates", None) or []
    for candidate in candidates:
        content = getattr(candidate, "content", None)
        if not content:
            continue
        parts = getattr(content, "parts", None) or []
        for part in parts:
            value = getattr(part, "text", None)
            if value:
                return value
    return ""


def run_file_analysis(
    *,
    file_path: str,
    mime_type: str,
    model_name: str | None = None,
    channel: str = "default",
) -> str:
    """Send the uploaded file to Finvela with the appropriate system prompt and return Markdown text."""
    app = current_app
    model_name = model_name or app.config.get("GEMINI_MODEL", "gemini-2.5-flash-lite")

    if channel == "whatsapp":
        system_prompt = WHATSAPP_CHAT_SYSTEM_PROMPT
        analysis_prompt = WHATSAPP_ANALYSIS_REQUEST_MESSAGE
    else:
        system_prompt = CHAT_SYSTEM_PROMPT
        analysis_prompt = ANALYSIS_REQUEST_MESSAGE

    gemini_file = gemini_client.upload_file(file_path, mime_type, app=app)
    model = gemini_client.build_model(
        model_name,
        app=app,
        system_instruction=system_prompt,
    )
    timeout = app.config.get("GEMINI_REQUEST_TIMEOUT", 90)
    response = model.generate_content(
        [
            {"role": "user", "parts": [{"text": analysis_prompt}]},
            {"role": "user", "parts": [gemini_file]},
        ],
        generation_config={"response_mime_type": "text/plain"},
        request_options={"timeout": timeout},
    )
    text = _extract_text(response)
    if not text.strip():
        raise RuntimeError("Finvela returned an empty response for the uploaded file")
    return text.strip()


def continue_chat(
    *,
    user_message: str,
    history: Sequence[dict[str, str]] | Iterable[dict[str, str]],
    model_name: str | None = None,
    channel: str = "default",
) -> str:
    """Send a user message with limited history and return the assistant Markdown reply."""
    app = current_app
    model_name = model_name or app.config.get("GEMINI_MODEL", "gemini-2.5-flash-lite")

    contents: list[dict[str, object]] = []
    if channel == "whatsapp":
        contents.append({"role": "user", "parts": [{"text": WHATSAPP_CHAT_STYLE_REMINDER}]})
    for item in history:
        role = item.get("role", "user")
        role = "model" if role == "assistant" else "user"
        contents.append({"role": role, "parts": [{"text": item.get("content", "")}]})
    contents.append({"role": "user", "parts": [{"text": user_message}]})

    model_kwargs = {}
    model = gemini_client.build_model(model_name, app=app, **model_kwargs)
    timeout = app.config.get("GEMINI_REQUEST_TIMEOUT", 90)
    response = model.generate_content(
        contents,
        generation_config={"response_mime_type": "text/plain"},
        request_options={"timeout": timeout},
    )
    text = _extract_text(response)
    if not text.strip():
        raise RuntimeError("Finvela returned an empty response for the chat prompt")
    return text.strip()
