"""Configuration constants for daily scorecard automation."""
import json
from datetime import date, timedelta
from pathlib import Path

# Hub/spoke exclusions - these sites are closed and should be filtered from all data
EXCLUDED_SITES = {"MNY-1", "FPK-1"}

# Hub roster with sort partners (ordered alphabetically)
HUB_ROSTER = {
    "ATL-13": "Staples",
    "CRN-1": "Staples",
    "DTX-1": "SpeedX",
    "EWR-2": "BroadRange",
    "GCO-1": "Staples",
    "GRE-1": "Staples",
    "LAX-11": "Razr",
    "LAX-12": "Warp",
    "MCO-1": "Staples",
    "NYC-1": "Staples",
    "ORD-7": "Warp",
}

# Spoke roster with sort partners (ordered alphabetically)
SPOKE_ROSTER = {
    "ATL-11": "Staples", "ATL-12": "Staples", "ATX-5": "Staples",
    "BKN-9": "BoxHero", "BLT-3": "Staples", "BNX-1": "Staples",
    "BOS-5": "Staples", "BOS-6": "Staples", "CHI-17": "Staples",
    "CIN-5": "Staples", "CLE-7": "Staples", "CLT-3": "Staples",
    "CLV-1": "Staples", "CNJ-2": "Staples", "COL-5": "Staples",
    "DAL-8": "Staples", "DAL-9": "Razr", "DCA-5": "Staples",
    "DET-13": "Staples", "EBY-3": "Dedicated", "HBG-2": "Staples",
    "HFD-3": "Staples", "HGR-1": "Staples", "HOU-10": "Staples",
    "HUD-1": "Staples", "INE-11": "Staples", "JAX-2": "Staples",
    "LAS-3": "Staples", "LAV-5": "Staples", "LAX-8": "Warp",
    "LAX-9": "Staples", "LIN-1": "Staples", "MSP-4": "Staples",
    "MSP-6": "Owen Allen Solutions", "NAS-2": "Staples",
    "NNJ-5": "Dedicated", "NNJ-6": "Staples", "OCO-1": "Razr",
    "ORL-3": "Staples", "PHL-7": "Staples", "PHL-8": "Flexe",
    "PHX-8": "Staples", "PIT-2": "Staples", "QNS-2": "Staples",
    "RAL-3": "Staples", "RIC-2": "Staples", "SAC-4": "Staples",
    "SAT-4": "Staples", "SND-3": "Staples", "TPA-4": "Staples",
    "VAB-4": "Staples",
}

# Sigma email subjects (searched in Gmail)
SIGMA_EMAILS = {
    "hub_forecasts": "Hub Forecasts",
    "hub_scanned": "Hub Scanned Volume",
    "cpt_metrics": "Controllable CPT Miss Metrics - Daily",
    "otd_misses": "OKR: Daily OTD Misses",
    "spoke_forecast_scanned": "Spoke Forecast & Scanned Volume",
}

# Slack channel IDs
SLACK_ASK_PARCELS_CHANNEL = "C04LU23A27N"
SLACK_TDD_CHANNEL = "C09MZ1QLCB0"

# Google Sheets
SHEETS_LIVEOPS_ID = "1l9RP11U1oFdNfLwpJmplQHceh8zfaT5pumPPAC8Ys9g"

_secrets_path = Path(__file__).resolve().parent.parent / "secrets.json"
if _secrets_path.exists():
    _secrets = json.loads(_secrets_path.read_text())
else:
    _secrets = {}

SHEETS_OAUTH_CLIENT_ID = _secrets.get("sheets_oauth_client_id", "")
SHEETS_OAUTH_CLIENT_SECRET = _secrets.get("sheets_oauth_client_secret", "")
SHEETS_REFRESH_TOKEN = _secrets.get("sheets_refresh_token", "")

# Thresholds
OTD_MISS_THRESHOLD = 0.35  # spoke miss % threshold for red/green

# Day of week names
DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# Month names
MONTHS = ["January", "February", "March", "April", "May", "June",
          "July", "August", "September", "October", "November", "December"]


def get_op_date(override: str = None) -> date:
    """Get the operational date (today by default, or parsed from YYYY-MM-DD)."""
    if override:
        return date.fromisoformat(override)
    return date.today()


def get_data_date(op_date: date) -> date:
    """Data date is always the day before the operational date."""
    return op_date - timedelta(days=1)


def format_date_long(d: date) -> str:
    """e.g. 'Wednesday, April 23, 2026'"""
    return f"{DAYS_OF_WEEK[d.weekday()]}, {MONTHS[d.month - 1]} {d.day}, {d.year}"


def format_date_short(d: date) -> str:
    """e.g. '4/23'"""
    return f"{d.month}/{d.day}"


def format_date_mmdd(d: date) -> str:
    """e.g. '4/23/2026'"""
    return f"{d.month}/{d.day}/{d.year}"


def format_date_gen(d: date) -> str:
    """e.g. 'Apr 23, 2026'"""
    short_months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    return f"{short_months[d.month - 1]} {d.day}, {d.year}"


def format_date_iso(d: date) -> str:
    """e.g. '2026-04-23'"""
    return d.isoformat()
