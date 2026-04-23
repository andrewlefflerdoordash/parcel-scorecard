"""Parsers for the 5 Sigma CSV email attachments.

Each function takes raw CSV text and a target date string (YYYY-MM-DD),
returning structured dicts. All parsers are defensive: they handle missing
columns, blank rows, encoding quirks, and NUL bytes without crashing.
"""
from __future__ import annotations

import csv
import io
import logging
import re
from datetime import date

from .config import EXCLUDED_SITES

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean_text(raw: str) -> str:
    """Strip NUL bytes and normalise line endings."""
    return raw.replace("\x00", "").replace("\r\n", "\n").replace("\r", "\n")


def _safe_decode(raw: str | bytes) -> str:
    if isinstance(raw, bytes):
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return raw.decode("latin-1")
    return raw


def _date_variants(target: str) -> list[str]:
    """Build plausible column-header representations of a date."""
    d = date.fromisoformat(target)
    return [
        d.isoformat(),                           # 2026-04-22
        f"{d.year}-{d.month:02d}-{d.day:02d}",
        f"{d.month}/{d.day}/{d.year}",            # 4/22/2026
        f"{d.month}/{d.day}/{d.year % 100:02d}",  # 4/22/26
        f"{d.month}/{d.day}",                      # 4/22
        f"{d.month:02d}/{d.day:02d}/{d.year}",    # 04/22/2026
        f"{d.month:02d}/{d.day:02d}",              # 04/22
    ]


def _find_date_col(headers: list[str], target: str) -> int | None:
    """Return the column index whose header matches target date, or None."""
    variants = set(v.strip().lower() for v in _date_variants(target))
    for idx, h in enumerate(headers):
        if h.strip().lower() in variants:
            return idx
    return None


def _find_date_col_in_rows(rows: list[list[str]], target: str, max_scan: int = 5) -> tuple[int | None, int | None]:
    """Scan the first few rows for a date column match.
    
    Returns (row_index, col_index) or (None, None).
    """
    for ri in range(min(max_scan, len(rows))):
        col = _find_date_col(rows[ri], target)
        if col is not None:
            return ri, col
    return None, None


def _parse_int(val: str) -> int:
    val = val.strip().replace(",", "").replace("%", "")
    if not val or val in ("-", "—", ""):
        return 0
    try:
        return int(round(float(val)))
    except ValueError:
        return 0


def _parse_float(val: str) -> float:
    val = val.strip().replace(",", "").replace("%", "")
    if not val or val in ("-", "—", ""):
        return 0.0
    try:
        return float(val)
    except ValueError:
        return 0.0


def _is_site_code(val: str) -> bool:
    return bool(re.match(r"^[A-Z]{2,5}-\d{1,3}$", val.strip()))


def _clean_hub_name(raw: str) -> str:
    """Strip suffixes like ' x-dock', ' x-dock cross dock' from hub names."""
    name = re.sub(r'\s*(x-dock\s*cross\s*dock|x-dock)\s*$', '', raw.strip(), flags=re.IGNORECASE)
    return name.strip()


def _rows_from_text(text: str) -> list[list[str]]:
    text = _clean_text(_safe_decode(text))
    reader = csv.reader(io.StringIO(text))
    return [row for row in reader]


# ---------------------------------------------------------------------------
# 1. Hub Forecasts
# ---------------------------------------------------------------------------

def parse_hub_forecasts(csv_text: str, target_date: str) -> dict[str, int]:
    """Parse hub forecast CSV.

    Real Sigma format:
      Row 0: "Sum of Hub Volume,Day of Hub Date,,,..."
      Row 1: "Hub Name,2026-01-28,2026-01-29,...,Total"
      Row 2+: "ATL-13,14761.0,12379.0,..."
    Hub names may have suffixes like "CRN-1 x-dock".
    """
    rows = _rows_from_text(csv_text)
    if not rows:
        log.warning("hub_forecasts: empty CSV")
        return {}

    header_ri, col = _find_date_col_in_rows(rows, target_date)
    if col is None:
        log.warning("hub_forecasts: date column not found for %s", target_date)
        return {}

    result: dict[str, int] = {}
    for row in rows[header_ri + 1:]:
        if len(row) <= col:
            continue
        raw_name = row[0].strip()
        if not raw_name:
            continue
        site = _clean_hub_name(raw_name)
        if site in EXCLUDED_SITES:
            continue
        if site.lower() in ("total", "network"):
            continue
        if _is_site_code(site):
            result[site] = result.get(site, 0) + _parse_int(row[col])

    log.info("hub_forecasts: parsed %d hubs for %s", len(result), target_date)
    return result


# ---------------------------------------------------------------------------
# 2. Hub Scanned Volume
# ---------------------------------------------------------------------------

def parse_hub_scanned(csv_text: str, target_date: str) -> dict[str, int]:
    """Parse hub scanned volume CSV.

    Real Sigma format (nested week grouping):
      Row 0: blank metadata
      Row 1: week headers with dates scattered
      Row 2: "Hub Name,,date1,date2,..." (actual column date headers)
      Row 3+: Per-hub blocks with sub-rows:
        "ATL-13,Forecast,14439,15124,..."
        ",First Scanned to Hub,12014,13097,..."
        ",Parcel Rollover 5pm Cutoff,..."
    
    We need the "First Scanned to Hub" row for each hub.
    """
    rows = _rows_from_text(csv_text)
    if not rows:
        log.warning("hub_scanned: empty CSV")
        return {}

    # Find the date column by scanning first few rows
    header_ri, col = _find_date_col_in_rows(rows, target_date, max_scan=5)
    if col is None:
        log.warning("hub_scanned: date column not found for %s", target_date)
        return {}

    result: dict[str, int] = {}
    current_hub = None

    for row in rows[header_ri + 1:]:
        if not row:
            continue
        cell0 = row[0].strip() if row[0] else ""
        cell1 = row[1].strip().lower() if len(row) > 1 else ""

        if cell0:
            cleaned = _clean_hub_name(cell0)
            if _is_site_code(cleaned) and cleaned not in EXCLUDED_SITES:
                current_hub = cleaned
            elif cleaned.lower() in ("total", "network"):
                current_hub = None
            else:
                current_hub = None

        if current_hub and "first scanned" in cell1:
            if col < len(row):
                vol = _parse_int(row[col])
                if vol > 0:
                    result[current_hub] = result.get(current_hub, 0) + vol

    # Fallback: simple layout (dates in header, sites in col 0)
    if not result:
        for row in rows[header_ri + 1:]:
            if len(row) <= col:
                continue
            site = _clean_hub_name(row[0].strip())
            if _is_site_code(site) and site not in EXCLUDED_SITES:
                result[site] = _parse_int(row[col])

    log.info("hub_scanned: parsed %d hubs for %s", len(result), target_date)
    return result


# ---------------------------------------------------------------------------
# 3. CPT Metrics
# ---------------------------------------------------------------------------

def parse_cpt_metrics(csv_text: str, target_date: str) -> dict[str, float]:
    """Parse controllable CPT miss metrics CSV.

    Real Sigma format:
      Row 0: "% OnTime CPT,Date,,,..."
      Row 1: "Hub,2026-04-22,2026-04-21,...,Total"
      Row 2+: "ATL-13,1.0,1.0,0.833333,..."
    
    Values are fractions (0-1), multiply by 100 for percentage.
    """
    rows = _rows_from_text(csv_text)
    if not rows:
        log.warning("cpt_metrics: empty CSV")
        return {}

    header_ri, col = _find_date_col_in_rows(rows, target_date)
    if col is None:
        log.warning("cpt_metrics: date column not found for %s", target_date)
        return {}

    result: dict[str, float] = {}
    for row in rows[header_ri + 1:]:
        if len(row) <= col:
            continue
        label = _clean_hub_name(row[0].strip())
        if not label or label in EXCLUDED_SITES:
            continue
        raw_val = _parse_float(row[col])
        if raw_val == 0.0 and row[col].strip() in ("", "-", "—"):
            continue

        pct = raw_val * 100 if raw_val <= 1.0 else raw_val

        if _is_site_code(label):
            result[label] = round(pct, 1)
        elif label.lower() in ("network", "total", "all"):
            result["Network"] = round(pct, 1)

    # Compute network average if not explicitly provided
    if "Network" not in result and result:
        hub_vals = [v for k, v in result.items() if _is_site_code(k) and v > 0]
        if hub_vals:
            result["Network"] = round(sum(hub_vals) / len(hub_vals), 1)

    log.info("cpt_metrics: parsed %d entries for %s", len(result), target_date)
    return result


# ---------------------------------------------------------------------------
# 4. OTD Misses
# ---------------------------------------------------------------------------

def parse_otd_misses(csv_text: str, target_date: str) -> dict:
    """Parse the OKR Daily OTD Misses CSV.

    Real Sigma format:
      Row 0: "% of Total for Sum of Barcodes (by Column),,,Day of TDD,,..."
      Row 1: "Step Type Bucket,Step Type Order,Facility or Route,2026-04-22,..."
      Row 2: "0. OTD,1. On Time Delivery,On Time Delivery,0.930578,..."
      ...later rows...
      "4. Spoke Sortation,4.x...,SPOKE-CODE,value,..."
      ",,...,SPOKE > ROUTE,value,..."

    Network OTD: fraction in the "0. OTD" row for the target date column.
    Spoke misses: rows where col 0 contains "4. Spoke Sortation" OR is empty
                  (continuation) while we're in section 4. Col 2 has facility.
                  Only keep simple spoke codes (not routes with ">").
    Values are fractions — multiply by 100 for percentages.
    """
    empty = {"network_otd_pct": None, "spoke_misses": {}}
    text = _clean_text(_safe_decode(csv_text))
    if not text.strip():
        log.warning("otd_misses: empty CSV")
        return empty

    rows = _rows_from_text(text)
    if not rows:
        return empty

    # Find the header row with date columns
    header_ri, date_col = _find_date_col_in_rows(rows, target_date, max_scan=5)
    if date_col is None:
        log.warning("otd_misses: date column not found for %s", target_date)
        return empty

    network_otd = None
    spoke_misses: dict[str, float] = {}
    in_spoke_section = False
    current_section = ""

    for row in rows[header_ri + 1:]:
        if len(row) <= date_col:
            continue

        col0 = row[0].strip() if row[0] else ""
        col2 = row[2].strip() if len(row) > 2 else ""

        # Track which section we're in
        if col0:
            section_match = re.match(r'^(\d+)\.\s', col0)
            if section_match:
                current_section = col0
                section_num = int(section_match.group(1))
                in_spoke_section = section_num == 4
            elif col0.lower().startswith("0. otd") or "on time delivery" in col0.lower():
                current_section = col0

        # Network OTD from "0. OTD" row
        if "0. otd" in current_section.lower() and "on time delivery" in col2.lower():
            raw = _parse_float(row[date_col])
            if 0 < raw <= 1:
                network_otd = round(raw * 100, 2)
            elif raw > 1:
                network_otd = round(raw, 2)

        # Spoke sortation miss attribution
        if in_spoke_section:
            # Skip route entries (contain ">"), only want simple spoke codes
            if ">" in col2:
                continue
            # Check if col2 is a clean spoke code
            cleaned = col2.strip()
            if _is_site_code(cleaned) and cleaned not in EXCLUDED_SITES:
                raw = _parse_float(row[date_col])
                if raw > 0:
                    pct = raw * 100 if raw < 1 else raw
                    spoke_misses[cleaned] = round(pct, 4)
                else:
                    spoke_misses[cleaned] = 0.0

        # Stop spoke section when we hit section 5+
        if col0 and re.match(r'^[5-9]\.\s', col0):
            in_spoke_section = False

    log.info("otd_misses: network_otd=%s, spoke_misses=%d sites for %s",
             network_otd, len(spoke_misses), target_date)
    return {"network_otd_pct": network_otd, "spoke_misses": spoke_misses}


# ---------------------------------------------------------------------------
# 5. Spoke Forecast & Scanned Volume
# ---------------------------------------------------------------------------

def parse_spoke_forecast_scanned(csv_text: str, target_date: str) -> dict:
    """Parse spoke forecast & scanned volume CSV.

    Handles multiple layouts:
    A) Simple: Site,Type,dates... (alternating Forecast/Scanned rows)
    B) Sigma nested: similar to hub scanned with sub-rows per spoke
    """
    empty: dict = {"forecast": {}, "scanned": {}}
    rows = _rows_from_text(csv_text)
    if not rows:
        log.warning("spoke_forecast_scanned: empty CSV")
        return empty

    header_ri, col = _find_date_col_in_rows(rows, target_date, max_scan=5)
    if col is None:
        log.warning("spoke_forecast_scanned: date column not found for %s", target_date)
        return empty

    forecast: dict[str, int] = {}
    scanned: dict[str, int] = {}
    current_site = None

    for row in rows[header_ri + 1:]:
        if len(row) <= col:
            continue

        cell0 = row[0].strip() if row[0] else ""
        cell1 = row[1].strip().lower() if len(row) > 1 else ""

        # New site starts when col 0 has a site code
        cleaned = _clean_hub_name(cell0) if cell0 else ""
        if cleaned and _is_site_code(cleaned):
            if cleaned in EXCLUDED_SITES:
                current_site = None
                continue
            if cleaned.lower() in ("total", "network"):
                current_site = None
                continue
            current_site = cleaned

        # Determine forecast vs scanned from cell1
        if current_site:
            if "forecast" in cell1:
                vol = _parse_int(row[col])
                if vol > 0:
                    forecast[current_site] = vol
            elif "scan" in cell1 or "first scanned" in cell1:
                vol = _parse_int(row[col])
                if vol > 0:
                    scanned[current_site] = vol

    # Fallback: if layout has Type in col 1 directly
    if not forecast and not scanned:
        for row in rows[header_ri + 1:]:
            if len(row) <= col or len(row) < 2:
                continue
            site_raw = row[0].strip()
            kind = row[1].strip().lower() if len(row) > 1 else ""
            site = _clean_hub_name(site_raw)
            if not _is_site_code(site) or site in EXCLUDED_SITES:
                continue
            if "forecast" in kind:
                forecast[site] = _parse_int(row[col])
            elif "scan" in kind:
                scanned[site] = _parse_int(row[col])

    log.info("spoke_forecast_scanned: %d forecasts, %d scanned for %s",
             len(forecast), len(scanned), target_date)
    return {"forecast": forecast, "scanned": scanned}
