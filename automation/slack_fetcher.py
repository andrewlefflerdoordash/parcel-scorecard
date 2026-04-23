"""Slack data fetching helpers for scorecard automation.

These functions generate search queries and parse Slack search results
into structured data for the dashboard.
"""
import re
from datetime import date


def ask_parcels_search_query(data_date: date) -> str:
    """Build Slack search query for #ask-parcels tickets.
    
    Args:
        data_date: The operational data date to search for
    Returns:
        Slack search query string  
    """
    date_str = f"{data_date.month}/{data_date.day}"
    after_ts = data_date.strftime("%Y-%m-%d")
    return f"in:ask-parcels after:{after_ts}"


def tdd_search_query(data_date: date) -> str:
    """Build Slack search query for TDD callouts.
    
    Args:
        data_date: The operational data date
    Returns:
        Slack search query string
    """
    date_str = f"{data_date.month}/{data_date.day}"
    return f"TDD {date_str}"


def parse_ask_parcels_results(search_results: str, data_date: date) -> dict:
    """Parse #ask-parcels search results into site-level ticket data.
    
    Args:
        search_results: Raw text from Slack search results
        data_date: Filter to this date's tickets
    Returns:
        dict mapping site_name -> list of ticket description strings
    """
    tickets = {}
    if not search_results or "No results found" in search_results:
        return tickets
    
    date_str = f"{data_date.month}/{data_date.day}"
    lines = search_results.split('\n')
    
    current_message = []
    in_message = False
    
    for line in lines:
        if line.startswith('### Result') or line.startswith('---'):
            if current_message:
                _extract_ticket_from_message('\n'.join(current_message), tickets)
                current_message = []
            in_message = True
        elif in_message:
            current_message.append(line)
    
    if current_message:
        _extract_ticket_from_message('\n'.join(current_message), tickets)
    
    return tickets


def _extract_ticket_from_message(message_text: str, tickets: dict):
    """Extract site and ticket info from a single Slack message."""
    site_pattern = re.compile(r'\b([A-Z]{2,5}-\d{1,2})\b')
    sites = site_pattern.findall(message_text)
    
    priority = "P1"
    p_match = re.search(r'\b(P[0-3])\b', message_text, re.IGNORECASE)
    if p_match:
        priority = p_match.group(1).upper()
    
    text_line = ""
    for line in message_text.split('\n'):
        if line.startswith('Text:'):
            text_line = line[5:].strip()
            break
    
    if not text_line:
        text_line = message_text[:200]
    
    # Clean up Slack formatting
    text_line = re.sub(r'<@[A-Z0-9]+\|?[^>]*>', '', text_line)
    text_line = re.sub(r'<https?://[^|>]+\|([^>]+)>', r'\1', text_line)
    text_line = re.sub(r'<https?://[^>]+>', '', text_line)
    text_line = text_line.strip()
    
    if text_line and sites:
        for site in set(sites):
            if site not in tickets:
                tickets[site] = []
            desc = f"{priority} &mdash; {text_line[:200]}"
            tickets[site].append(desc)


def parse_tdd_results(search_results: str, data_date: date) -> dict:
    """Parse TDD search results into site-level callout data.
    
    Args:
        search_results: Raw text from Slack search results
        data_date: Filter date
    Returns:
        dict mapping site_name -> callout description string
    """
    callouts = {}
    if not search_results or "No results found" in search_results:
        return callouts
    
    # TDD messages typically have format: SITE // Category\nDescription
    site_pattern = re.compile(r'([A-Z]{2,5}-\d{1,2})\s*//\s*(.+)')
    
    lines = search_results.split('\n')
    current_site = None
    current_category = None
    
    for line in lines:
        match = site_pattern.search(line)
        if match:
            current_site = match.group(1)
            current_category = match.group(2).strip()
        elif current_site and line.strip() and not line.startswith('#') and not line.startswith('---'):
            desc = line.strip()
            if desc and current_category:
                callouts[current_site] = f'<span class="tag tag-tdd">TDD</span> {current_category} &mdash; {desc}'
                current_site = None
                current_category = None
    
    return callouts
