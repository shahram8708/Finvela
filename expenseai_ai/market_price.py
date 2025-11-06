"""Market price benchmarking using Google Finvela with grounding."""
from __future__ import annotations

import json
import re
import sys
from collections.abc import Iterable as IterableABC
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, Optional

from flask import Flask, current_app

try:  # pragma: no cover - optional dependency during static analysis
    from google import genai
    from google.genai import types
except Exception as exc:  # pragma: no cover - defensive for missing dependency
    genai = None  # type: ignore[assignment]
    types = None  # type: ignore[assignment]
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None

_client_cache: Any | None = None
_client_settings: Dict[str, Any] | None = None

_PROMPT_TEMPLATE = (
    "You are assisting an accounts payable analyst in validating invoice pricing.\n"
    "Carefully research the *current* market price using Google Web Search grounding for the described item.\n"
    "You must provide a grounded numeric estimate even when exact matches are unavailable: compare neighbouring services, historic price lists, vendor quotes, or government fee schedules, and derive the closest current INR per-unit price.\n"
    "Always include at least one numeric value for market_price.amount (in the same currency) and never respond with null unless all grounded sources completely lack numbers.\n"
    "If data is sparse, produce the best estimate you can with a low confidence score rather than declining.\n"
    "Return STRICT JSON with this shape (null only when absolutely no numeric information exists after searching):\n"
    "{\n"
    "  \"product_name\": string,\n"
    "  \"search_query\": string,\n"
    "  \"market_price\": {\"amount\": number|null, \"currency\": string},\n"
    "  \"price_range\": {\"low\": number|null, \"high\": number|null, \"currency\": string},\n"
    "  \"confidence\": number|null,\n"
    "  \"summary\": string,\n"
    "  \"sources\": [\n"
    "    {\"title\": string, \"url\": string, \"price\": number|null, \"currency\": string}\n"
    "  ]\n"
    "}\n"
    "Use a per-unit comparison that matches the invoice unit price if possible.\n"
    "If you provide an estimate, cite the specific URLs and note any assumptions in the summary."
)


def _get_client(app: Flask | None = None) -> Any:
    """Return a cached Finvela client configured with the API key."""
    if genai is None or types is None:  # pragma: no cover - handled at runtime
        raise RuntimeError(
            "google-genai is not installed or failed to import"
        ) from _IMPORT_ERROR

    app = app or current_app
    api_key = app.config.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not configured")

    global _client_cache, _client_settings
    desired = {"api_key": api_key}
    if _client_cache is not None and _client_settings == desired:
        return _client_cache

    _client_cache = genai.Client(api_key=api_key)
    _client_settings = desired
    return _client_cache


def _select_text(response: Any) -> str:
    """Extract raw text from a Finvela response."""
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


_JSON_BLOCK = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)


def _extract_json_candidates(text: str) -> Iterable[str]:
    """Yield plausible JSON object snippets from the raw model text."""

    if not text:
        return []

    trimmed = text.strip()
    candidates: list[str] = []

    # Direct attempt
    candidates.append(trimmed)

    # Fenced code block
    for match in _JSON_BLOCK.finditer(trimmed):
        snippet = match.group(1).strip()
        if snippet:
            candidates.append(snippet)

    # Scan for first balanced JSON object
    depth = 0
    start = None
    for index, char in enumerate(trimmed):
        if char == "{":
            if depth == 0:
                start = index
            depth += 1
        elif char == "}":
            if depth:
                depth -= 1
                if depth == 0 and start is not None:
                    snippet = trimmed[start : index + 1].strip()
                    if snippet:
                        candidates.append(snippet)
                    start = None
    return candidates


def _parse_json(text: str) -> Dict[str, Any]:
    """Best-effort JSON extraction from model output."""
    if not text:
        return {}
    for candidate in _extract_json_candidates(text):
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    return {}


_NUMBER_PATTERN = re.compile(r"-?\d+(?:\.\d+)?")
_CURRENCY_PATTERN = re.compile(r"([A-Z]{3})")


def _to_decimal(value: Any) -> Decimal | None:
    """Convert loose numeric representations into Decimal."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        cleaned = cleaned.replace(",", "").replace("₹", "").replace("$", "").strip()
        match = _NUMBER_PATTERN.search(cleaned)
        if not match:
            return None
        cleaned = match.group(0)
        try:
            return Decimal(cleaned)
        except InvalidOperation:
            return None
    return None


def _extract_amount(node: Any) -> Decimal | None:
    """Attempt to coerce various nested structures into a Decimal amount."""

    if node is None:
        return None
    if isinstance(node, Decimal):
        return node
    if isinstance(node, (int, float, str)):
        return _to_decimal(node)
    if isinstance(node, dict):
        for key in ("amount", "value", "price", "avg", "average", "median", "mean"):
            if key in node:
                extracted = _extract_amount(node.get(key))
                if extracted is not None:
                    return extracted
        for key in ("text", "formatted", "display"):
            if key in node:
                extracted = _to_decimal(node.get(key))
                if extracted is not None:
                    return extracted
        return None
    if isinstance(node, IterableABC) and not isinstance(node, (bytes, bytearray, str)):
        for entry in node:
            extracted = _extract_amount(entry)
            if extracted is not None:
                return extracted
    return None


def _extract_currency(node: Any, default: str) -> str:
    """Return a plausible ISO currency code from nested data."""

    if isinstance(node, dict):
        for key in ("currency", "code", "unit"):
            candidate = node.get(key)
            if isinstance(candidate, str) and candidate.strip():
                text = candidate.strip().upper()
                if len(text) == 3 and text.isalpha():
                    return text
                match = _CURRENCY_PATTERN.search(text)
                if match:
                    return match.group(1)
        for value in node.values():
            derived = _extract_currency(value, default)
            if derived != default:
                return derived
        return default
    if isinstance(node, (list, tuple)):
        for entry in node:
            derived = _extract_currency(entry, default)
            if derived != default:
                return derived
        return default
    if isinstance(node, str) and node.strip():
        text = node.strip().upper()
        if len(text) == 3 and text.isalpha():
            return text
        match = _CURRENCY_PATTERN.search(text)
        if match:
            return match.group(1)
    return default


def _compute_delta(billed: Decimal | None, market: Decimal | None) -> Optional[float]:
    if billed is None or market is None:
        return None
    if market == 0:
        return None
    delta = (billed - market) / market * Decimal("100")
    return float(round(delta, 4))


def benchmark_line_item(
    *,
    description: str,
    billed_price: Decimal | None,
    currency: str,
    quantity: Decimal | None = None,
    model_name: str | None = None,
    app: Flask | None = None,
) -> Dict[str, Any]:
    """Call Finvela with Google Search grounding to fetch market pricing."""

    app = app or current_app
    client = _get_client(app)
    model = model_name or app.config.get("GEMINI_MODEL", "gemini-2.5-flash-lite")

    normalized_desc = (description or "Unnamed product").strip()
    if not normalized_desc:
        normalized_desc = "Unnamed product"
    normalized_currency = (currency or "INR").strip().upper() or "INR"
    billed_str = f"{billed_price}" if billed_price is not None else "unknown"
    qty_str = f"{quantity}" if quantity is not None else "unknown"

    prompt = (
        f"Invoice line item description: {normalized_desc}.\n"
        f"Billed unit price: {billed_str} {normalized_currency}. Quantity: {qty_str}.\n"
        "Determine a comparable market unit price now."
    )

    # Latest Finvela releases disallow combining JSON-mode responses with tool use.
    config = types.GenerateContentConfig(
        tools=[types.Tool(google_search=types.GoogleSearch())],
        response_mime_type="application/json",
    )

    try:
        response = client.models.generate_content(
            model=model,
            contents=f"{_PROMPT_TEMPLATE}\n\n{prompt}",
            config=config,
        )
    except Exception as exc:
        error_text = str(exc)
        message = error_text.lower()
        if "response mime type" not in message or "unsupported" not in message:
            raise

        # Fallback: retry without forcing JSON mode so tool use remains enabled.
        if app:
            app.logger.warning(
                "market_price.json_mode_fallback",
                extra={"error": error_text},
            )
        config = types.GenerateContentConfig(
            tools=[types.Tool(google_search=types.GoogleSearch())]
        )
        response = client.models.generate_content(
            model=model,
            contents=f"{_PROMPT_TEMPLATE}\n\n{prompt}",
            config=config,
        )

    raw_text = _select_text(response)
    parsed = _parse_json(raw_text)

    if app.config.get("MARKET_PRICE_DEBUG", False):
        try:
            pretty_payload = json.dumps(parsed, indent=2, default=str)
        except TypeError:
            pretty_payload = str(parsed)

        debug_message = (
            "=== Market price benchmark response ===\n"
            "Prompt:\n"
            f"{prompt}\n"
            "Raw text:\n"
            f"{raw_text}\n"
            "Parsed JSON:\n"
            f"{pretty_payload}\n"
            "======================================"
        )

        sys.stdout.write(debug_message + "\n")
        sys.stdout.flush()
        app.logger.info(
            "market_price.debug",
            extra={
                "prompt": prompt,
                "raw_text": raw_text,
                "parsed": parsed,
            },
        )
    market_payload = parsed.get("market_price")
    range_payload = parsed.get("price_range")
    sources_payload = parsed.get("sources") or []

    market_amount = _extract_amount(market_payload)
    range_low = None
    range_high = None
    if isinstance(range_payload, dict):
        range_low = _extract_amount(range_payload.get("low"))
        range_high = _extract_amount(range_payload.get("high"))
    else:
        range_low = _extract_amount(parsed.get("price_low"))
        range_high = _extract_amount(parsed.get("price_high"))
        fallback_range = _extract_amount(range_payload)
        if range_low is None:
            range_low = fallback_range
        if range_high is None:
            range_high = fallback_range

    if market_amount is None:
        for key in (
            "market_price_amount",
            "market_price_value",
            "market_price_avg",
            "market_price_average",
            "marketPrice",
            "marketPriceAmount",
            "market_avg_price",
        ):
            market_amount = _extract_amount(parsed.get(key))
            if market_amount is not None:
                break

    if market_amount is None:
        if range_low is not None and range_high is not None:
            market_amount = (range_low + range_high) / Decimal("2")
        elif range_low is not None:
            market_amount = range_low
        elif range_high is not None:
            market_amount = range_high

    market_currency = _extract_currency(market_payload, normalized_currency)
    if market_currency == normalized_currency and isinstance(range_payload, dict):
        market_currency = _extract_currency(range_payload, market_currency)
    if market_currency == normalized_currency:
        market_currency = _extract_currency(parsed.get("currency"), market_currency)

    delta_percent = _compute_delta(billed_price, market_amount)

    normalized_sources: list[dict[str, Any]] = []
    if isinstance(sources_payload, IterableABC):
        for entry in sources_payload:
            if not isinstance(entry, dict):
                continue
            price_value = _extract_amount(entry.get("price"))
            if price_value is None:
                price_value = _extract_amount(entry)
            normalized_sources.append(
                {
                    "title": str(entry.get("title", "Source")).strip() or "Source",
                    "url": str(entry.get("url", "")).strip(),
                    "price": price_value,
                    "currency": _extract_currency(entry, market_currency or normalized_currency),
                }
            )

    return {
        "product_name": parsed.get("product_name") or normalized_desc,
        "search_query": parsed.get("search_query") or normalized_desc,
        "market_price": market_amount,
        "market_currency": market_currency,
        "price_low": range_low,
        "price_high": range_high,
        "delta_percent": delta_percent,
        "summary": parsed.get("summary") or "",
        "confidence": parsed.get("confidence"),
        "sources": normalized_sources,
        "raw_response": {
            "text": raw_text,
            "parsed": parsed,
        },
    }
