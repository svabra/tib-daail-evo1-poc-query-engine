from __future__ import annotations

from html import escape
from typing import Any

from ...models import PythonJobOutputDefinition


ALLOWED_HTML_TAGS = {
    "a",
    "b",
    "blockquote",
    "br",
    "caption",
    "code",
    "div",
    "em",
    "figcaption",
    "figure",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hr",
    "i",
    "img",
    "li",
    "ol",
    "p",
    "pre",
    "span",
    "strong",
    "sub",
    "sup",
    "table",
    "tbody",
    "td",
    "th",
    "thead",
    "tr",
    "ul",
}
ALLOWED_HTML_ATTRIBUTES = {
    "*": ["class"],
    "a": ["href", "target", "rel"],
    "img": ["alt", "src"],
}


def sanitize_html_fragment(value: str) -> str:
    raw_value = str(value or "")
    try:
        import bleach
    except ImportError:
        return f"<pre>{escape(raw_value)}</pre>"

    return bleach.clean(
        raw_value,
        tags=ALLOWED_HTML_TAGS,
        attributes=ALLOWED_HTML_ATTRIBUTES,
        protocols={"http", "https", "data"},
        strip=True,
    )


def mime_bundle_to_output(data: dict[str, Any], metadata: dict[str, Any] | None = None) -> PythonJobOutputDefinition | None:
    bundle = dict(data or {})
    _metadata = dict(metadata or {})

    image_png = bundle.get("image/png")
    if image_png:
        return PythonJobOutputDefinition(
            output_type="image",
            data=str(image_png),
            mime_type="image/png",
        )

    json_data = bundle.get("application/json")
    if json_data is not None:
        return PythonJobOutputDefinition(
            output_type="json",
            data=json_data,
            mime_type="application/json",
        )

    html_data = str(bundle.get("text/html") or "").strip()
    plain_text = str(bundle.get("text/plain") or "").strip()
    if html_data:
        output_type = "table" if "<table" in html_data.lower() else "html"
        return PythonJobOutputDefinition(
            output_type=output_type,
            html=sanitize_html_fragment(html_data),
            text=plain_text,
            mime_type="text/html",
        )

    if plain_text:
        return PythonJobOutputDefinition(
            output_type="text",
            text=plain_text,
            mime_type="text/plain",
        )

    return None


def stream_output(name: str, text: str) -> PythonJobOutputDefinition:
    return PythonJobOutputDefinition(
        output_type="stream",
        name=str(name or "").strip() or "stdout",
        text=str(text or ""),
    )


def error_output(ename: str, evalue: str, traceback: list[str] | None = None) -> PythonJobOutputDefinition:
    return PythonJobOutputDefinition(
        output_type="error",
        error_name=str(ename or "").strip(),
        error_value=str(evalue or ""),
        traceback=[str(line) for line in (traceback or [])],
        text="\n".join(str(line) for line in (traceback or []) if str(line).strip()) or str(evalue or ""),
    )
