"""Gmail data fetching helpers for scorecard automation.

These functions generate search queries and parse results.
Actual MCP calls are made by the AI agent orchestrator.
"""
import base64
import json
import re


def sigma_search_query(subject: str, after_date: str) -> str:
    """Build Gmail search query for a Sigma export email.
    
    Args:
        subject: Email subject (e.g. "Hub Forecasts")
        after_date: Date string YYYY/MM/DD format for Gmail search
    Returns:
        Gmail search query string
    """
    return f'subject:"{subject}" from:sigmacomputing after:{after_date} has:attachment'


def eos_search_query(after_date: str) -> str:
    """Build Gmail search query for EOS reports.
    
    Args:
        after_date: Date string YYYY/MM/DD format
    Returns:
        Gmail search query string
    """
    return f'subject:EOS after:{after_date}'


def decode_attachment(base64_data: str) -> bytes:
    """Decode a base64-encoded email attachment."""
    # Gmail uses URL-safe base64
    padded = base64_data + '=' * (4 - len(base64_data) % 4)
    try:
        return base64.urlsafe_b64decode(padded)
    except Exception:
        return base64.b64decode(padded)


def clean_csv_bytes(raw: bytes) -> str:
    """Clean raw CSV bytes: remove NUL bytes, handle encoding."""
    cleaned = raw.replace(b'\x00', b'')
    try:
        return cleaned.decode('utf-8')
    except UnicodeDecodeError:
        return cleaned.decode('latin-1')


def parse_eos_summary(email_body: str, site_name: str) -> str:
    """Extract a brief EOS summary from an email body.
    
    Returns a short summary string suitable for the EOS Notes column.
    """
    if not email_body:
        return ""
    # Truncate very long bodies
    text = email_body[:2000]
    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def identify_hub_from_eos(subject: str, sender: str, body: str) -> str:
    """Try to identify which hub an EOS email is for.
    
    Looks at subject line and body for hub identifiers like LAX-11, DTX-1, etc.
    """
    combined = f"{subject} {sender} {body[:500]}"
    # Match patterns like LAX-11, DTX-1, EWR-2, etc.
    hub_pattern = re.compile(r'\b([A-Z]{2,4}-\d{1,2})\b')
    matches = hub_pattern.findall(combined)
    if matches:
        return matches[0]
    alt_names = {
        "SpeedX": "DTX-1", "BroadRange": "EWR-2", "Dedicated": "LAX-11",
    }
    for alt, hub in alt_names.items():
        if alt.lower() in combined.lower():
            return hub
    return ""
