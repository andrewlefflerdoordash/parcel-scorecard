"""Google Sheets data fetching helpers for scorecard automation.

Fetches Roll Tracking and Pass Down data from the LiveOps Google Sheet
using OAuth2 and the Sheets API v4.
"""
import json
import re
from datetime import date
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote


TOKEN_URL = "https://oauth2.googleapis.com/token"
SHEETS_API = "https://sheets.googleapis.com/v4/spreadsheets"


def get_access_token(client_id: str, client_secret: str, refresh_token: str) -> str:
    """Exchange refresh token for a fresh access token."""
    data = urlencode({
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }).encode()
    req = Request(TOKEN_URL, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urlopen(req) as resp:
        result = json.loads(resp.read())
    return result["access_token"]


def fetch_sheet_range(access_token: str, sheet_id: str, range_name: str) -> list:
    """Fetch a range from a Google Sheet, returns list of rows."""
    url = f"{SHEETS_API}/{sheet_id}/values/{quote(range_name)}"
    req = Request(url)
    req.add_header("Authorization", f"Bearer {access_token}")
    with urlopen(req) as resp:
        result = json.loads(resp.read())
    return result.get("values", [])


def parse_roll_tracking(rows: list, data_date: date) -> list:
    """Parse Roll Tracking rows, filter to data_date.
    
    Returns list of dicts with keys: status, origin, destination, 
    shipment, pallets, units, root_cause, details, submitted_by
    """
    if not rows:
        return []
    
    header = rows[0] if rows else []
    entries = []
    date_str_slash = f"{data_date.month:02d}/{data_date.day:02d}/{str(data_date.year)[2:]}"
    date_str_slash2 = f"{data_date.month:02d}/{data_date.day:02d}/{data_date.year}"
    date_str_short = f"{data_date.month}/{data_date.day}"
    
    for row in rows[1:]:
        if len(row) < 6:
            continue
        row_text = ' '.join(str(c) for c in row)
        if date_str_slash in row_text or date_str_slash2 in row_text or date_str_short in row_text:
            entry = {
                "status": row[0] if len(row) > 0 else "",
                "ship_date": row[1] if len(row) > 1 else "",
                "delivery_date": row[2] if len(row) > 2 else "",
                "carrier": row[3] if len(row) > 3 else "",
                "lane_type": row[4] if len(row) > 4 else "",
                "origin": row[5] if len(row) > 5 else "",
                "destination": row[6] if len(row) > 6 else "",
                "shipment": row[7] if len(row) > 7 else "",
                "pallets": row[8] if len(row) > 8 else "",
                "units": row[9] if len(row) > 9 else "",
                "root_cause": row[10] if len(row) > 10 else "",
                "details": row[11] if len(row) > 11 else "",
            }
            entries.append(entry)
    
    return entries


def parse_pass_down(rows: list, data_date: date) -> list:
    """Parse Pass Down rows, filter to data_date.
    
    Returns list of dicts with keys: date, issues, resolved, 
    next_shift_impact, action_items, submitted_by
    """
    if not rows:
        return []
    
    entries = []
    date_str_slash = f"{data_date.month:02d}/{data_date.day:02d}/{str(data_date.year)[2:]}"
    date_str_slash2 = f"{data_date.month:02d}/{data_date.day:02d}/{data_date.year}"
    date_str_short = f"{data_date.month}/{data_date.day}"
    
    for row in rows[1:]:
        if not row:
            continue
        date_cell = str(row[0]) if row else ""
        if date_str_slash in date_cell or date_str_slash2 in date_cell or date_str_short in date_cell:
            entry = {
                "date": date_cell,
                "issues": row[1] if len(row) > 1 else "",
                "resolved": row[2] if len(row) > 2 else "",
                "next_shift": row[3] if len(row) > 3 else "",
                "action_items": row[4] if len(row) > 4 else "",
                "submitted_by": row[5] if len(row) > 5 else "",
            }
            entries.append(entry)
    
    return entries


def map_roll_to_sites(roll_entries: list) -> dict:
    """Map roll tracking entries to site-level LiveOps strings.
    
    Returns dict mapping site_name -> list of HTML description strings.
    """
    site_liveops = {}
    
    for entry in roll_entries:
        origin = entry.get("origin", "").strip()
        dest = entry.get("destination", "").strip()
        carrier = entry.get("carrier", "").strip()
        pallets = entry.get("pallets", "").strip()
        units = entry.get("units", "").strip()
        root_cause = entry.get("root_cause", "").strip()
        details = entry.get("details", "").strip()[:150]
        
        vol_parts = []
        if pallets:
            vol_parts.append(f"{pallets} plts")
        if units:
            vol_parts.append(f"{units} units")
        vol_str = "/".join(vol_parts)
        
        cause_str = f", {root_cause}" if root_cause else ""
        detail_str = f" &mdash; {details}" if details else ""
        
        desc = f"Roll &mdash; {origin}&rarr;{dest}"
        if carrier:
            desc += f" ({carrier}"
            if vol_str:
                desc += f", {vol_str}"
            desc += f"{cause_str}{detail_str})"
        
        for site in [origin, dest]:
            site_clean = re.sub(r'\s*/\s*', '/', site)
            for s in site_clean.split('/'):
                s = s.strip()
                if s and re.match(r'^[A-Z]{2,5}-\d{1,2}$', s):
                    if s not in site_liveops:
                        site_liveops[s] = []
                    site_liveops[s].append(desc)
    
    return site_liveops


def map_passdown_to_sites(pass_entries: list) -> dict:
    """Map pass down entries to site-level LiveOps strings.
    
    Returns dict mapping site_name -> list of HTML description strings.
    """
    site_liveops = {}
    site_pattern = re.compile(r'\b([A-Z]{2,5}-\d{1,2})\b')
    
    for entry in pass_entries:
        issues = entry.get("issues", "")
        next_shift = entry.get("next_shift", "")
        combined = f"{issues} {next_shift}"
        
        sites_found = site_pattern.findall(combined)
        
        summary = issues[:200] if issues else next_shift[:200]
        summary = re.sub(r'[@<][^>]*>', '', summary).strip()
        summary = re.sub(r'\s+', ' ', summary)
        
        if summary and sites_found:
            for site in set(sites_found):
                if site not in site_liveops:
                    site_liveops[site] = []
                site_liveops[site].append(f"Pass Down &mdash; {summary}")
    
    return site_liveops
