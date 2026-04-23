"""Render the daily scorecard HTML from structured data."""
import json
from datetime import date
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

TEMPLATE_DIR = Path(__file__).resolve().parent.parent
TEMPLATE_NAME = "template.html.j2"


def color_for_pct(value: float, good_threshold: float = 95, warn_threshold: float = 90) -> str:
    """Return CSS variable name for a percentage value."""
    if value >= good_threshold:
        return "var(--green)"
    elif value >= warn_threshold:
        return "var(--yellow)"
    return "var(--red)"


def color_for_var(var_pct: float) -> str:
    """Color for forecast variance: green if close to 0, red if large."""
    if abs(var_pct) <= 10:
        return "var(--green)"
    elif abs(var_pct) <= 25:
        return "var(--yellow)"
    return "var(--red)"


def trend_info(current: float, prior: float, higher_is_better: bool = True) -> tuple:
    """Compute trend text and CSS class.
    
    Returns (trend_text, trend_class) tuple.
    """
    if prior is None:
        return (f"No prior day data", "neutral")
    
    diff = current - prior
    if abs(diff) < 0.001:
        return (f"Same as prior day ({prior})", "neutral")
    
    if higher_is_better:
        improved = diff > 0
    else:
        improved = diff < 0
    
    if improved:
        return (f"↑ Improved v. prior day ({prior})", "up")
    else:
        return (f"↓ Worsened v. prior day ({prior})", "down")


def trend_info_int(current: int, prior: int, higher_is_better: bool = True) -> tuple:
    """Compute trend for integer values."""
    if prior is None:
        return ("No prior day data", "neutral")
    if current == prior:
        return (f"Same as prior day ({prior})", "neutral")
    improved = (current > prior) if higher_is_better else (current < prior)
    if improved:
        return (f"↑ Improved v. prior day ({prior})", "up")
    else:
        return (f"↓ Worsened v. prior day ({prior})", "down")


def format_number(n) -> str:
    """Format a number with commas, or return dash if None."""
    if n is None:
        return "—"
    try:
        return f"{int(n):,}"
    except (ValueError, TypeError):
        return str(n)


def compute_var_pct(forecast: int, scanned: int) -> tuple:
    """Compute variance percentage and return (display_str, color)."""
    if not forecast or not scanned:
        return ("—", "var(--dim)")
    var = ((scanned - forecast) / forecast) * 100
    sign = "+" if var > 0 else ""
    return (f"{sign}{var:.1f}%", color_for_var(var))


def build_kpi_tiles(data: dict) -> list:
    """Build the KPI tile data from the aggregated dashboard data.
    
    Args:
        data: dict with keys like network_otd_pct, network_cpt_pct, 
              hub_forecast_total, hub_scanned_total, spoke_forecast_total,
              spoke_scanned_total, spokes_above_threshold, spokes_below_threshold,
              eos_received_count, eos_total_count, eos_hub_names,
              ticket_site_count, ticket_total, ticket_breakdown,
              roll_count, passdown_count, and prior_day versions of each
    Returns:
        list of KPI tile dicts
    """
    kpis = []
    
    # 1. Network OTD %
    otd = data.get("network_otd_pct")
    prior_otd = data.get("prior_network_otd_pct")
    otd_display = f"{otd:.2f}%" if otd is not None else "—"
    otd_color = color_for_pct(otd, 95, 90) if otd is not None else "var(--dim)"
    otd_trend = trend_info(otd, prior_otd) if otd is not None else ("Pending", "neutral")
    kpis.append({
        "label": "Network OTD %",
        "value": otd_display,
        "color": otd_color,
        "sub": f"OKR Daily OTD Misses ({data.get('data_date_short', '')})",
        "trend_text": otd_trend[0],
        "trend_class": otd_trend[1],
    })
    
    # 2. Network Hub CPT %
    cpt = data.get("network_cpt_pct")
    prior_cpt = data.get("prior_network_cpt_pct")
    cpt_display = f"{cpt:.1f}%" if cpt is not None else "—"
    cpt_color = color_for_pct(cpt, 95, 85) if cpt is not None else "var(--dim)"
    cpt_trend = trend_info(cpt, prior_cpt) if cpt is not None else ("Pending", "neutral")
    kpis.append({
        "label": "Network Hub CPT %",
        "value": cpt_display,
        "color": cpt_color,
        "sub": f"Controllable CPT ({data.get('data_date_short', '')})",
        "trend_text": cpt_trend[0],
        "trend_class": cpt_trend[1],
    })
    
    # 3. Spoke Forecast Var %
    sf = data.get("spoke_forecast_total", 0)
    ss = data.get("spoke_scanned_total", 0)
    if sf and ss:
        spoke_var = ((ss - sf) / sf) * 100
        spoke_var_display = f"{'+' if spoke_var > 0 else ''}{spoke_var:.1f}%"
        spoke_var_color = color_for_var(spoke_var)
        spoke_sub = f"{format_number(sf)} forecast / {format_number(ss)} scanned"
    else:
        spoke_var = None
        spoke_var_display = "—"
        spoke_var_color = "var(--dim)"
        spoke_sub = "Spoke data pending"
    prior_spoke_var = data.get("prior_spoke_forecast_var")
    spoke_trend = trend_info(abs(spoke_var) if spoke_var else 0, abs(prior_spoke_var) if prior_spoke_var else None, higher_is_better=False) if spoke_var is not None else ("Pending", "neutral")
    kpis.append({
        "label": "Spoke Forecast Var %",
        "value": spoke_var_display,
        "color": spoke_var_color,
        "sub": spoke_sub,
        "trend_text": spoke_trend[0],
        "trend_class": spoke_trend[1],
    })
    
    # 4. Hub Forecast Var %
    hf = data.get("hub_forecast_total", 0)
    hs = data.get("hub_scanned_total", 0)
    if hf and hs:
        hub_var = ((hs - hf) / hf) * 100
        hub_var_display = f"{'+' if hub_var > 0 else ''}{hub_var:.1f}%"
        hub_var_color = color_for_var(hub_var)
        hub_sub = f"{format_number(hf)} forecast / {format_number(hs)} scanned"
    else:
        hub_var = None
        hub_var_display = "—"
        hub_var_color = "var(--dim)"
        hub_sub = "Hub data pending"
    prior_hub_var = data.get("prior_hub_forecast_var")
    hub_trend = trend_info(abs(hub_var) if hub_var else 0, abs(prior_hub_var) if prior_hub_var else None, higher_is_better=False) if hub_var is not None else ("Pending", "neutral")
    kpis.append({
        "label": "Hub Forecast Var %",
        "value": hub_var_display,
        "color": hub_var_color,
        "sub": hub_sub,
        "trend_text": hub_trend[0],
        "trend_class": hub_trend[1],
    })
    
    # 5. Spokes w/OTD Miss > 0.35%
    above = data.get("spokes_above_threshold", 0)
    prior_above = data.get("prior_spokes_above_threshold")
    total_spokes = data.get("total_reporting_spokes", 0)
    kpis.append({
        "label": "# Spokes w/OTD Miss > 0.35%",
        "value": str(above),
        "color": "var(--green)" if above == 0 else "var(--red)",
        "sub": f"of {total_spokes} spokes ({data.get('data_date_short', '')})",
        "trend_text": trend_info_int(above, prior_above, higher_is_better=False)[0],
        "trend_class": trend_info_int(above, prior_above, higher_is_better=False)[1],
    })
    
    # 6. Clean Spokes
    below = data.get("spokes_below_threshold", 0)
    prior_below = data.get("prior_spokes_below_threshold")
    kpis.append({
        "label": "# Clean Spokes w/OTD Miss < 0.35%",
        "value": str(below),
        "color": "var(--green)",
        "sub": f"≤0.35% OTD miss attribution ({data.get('data_date_short', '')})",
        "trend_text": trend_info_int(below, prior_below, higher_is_better=True)[0],
        "trend_class": trend_info_int(below, prior_below, higher_is_better=True)[1],
    })
    
    # 7. EOS Reports
    eos_recv = data.get("eos_received_count", 0)
    eos_total = data.get("eos_total_count", len(data.get("hubs", [])))
    eos_names = data.get("eos_hub_names", [])
    prior_eos = data.get("prior_eos_received")
    eos_color = "var(--green)" if eos_recv == eos_total else "var(--yellow)" if eos_recv > 0 else "var(--red)"
    kpis.append({
        "label": "# of Hub EOS Reports Received",
        "value": f"{eos_recv} / {eos_total}",
        "color": eos_color,
        "sub": ", ".join(eos_names) if eos_names else "None received",
        "trend_text": trend_info_int(eos_recv, prior_eos, higher_is_better=True)[0],
        "trend_class": trend_info_int(eos_recv, prior_eos, higher_is_better=True)[1],
    })
    
    # 8. Ask-Parcel Tickets
    ticket_sites = data.get("ticket_site_count", 0)
    ticket_total = data.get("ticket_total", 0)
    ticket_breakdown = data.get("ticket_breakdown", "")
    prior_ticket_sites = data.get("prior_ticket_site_count")
    kpis.append({
        "label": "# of Sites w/Ask-Parcel Tickets",
        "value": str(ticket_sites),
        "color": "var(--green)" if ticket_sites == 0 else "var(--red)",
        "sub": f"{ticket_total} tickets: {ticket_breakdown}" if ticket_total else "No tickets",
        "trend_text": trend_info_int(ticket_sites, prior_ticket_sites, higher_is_better=False)[0],
        "trend_class": trend_info_int(ticket_sites, prior_ticket_sites, higher_is_better=False)[1],
    })
    
    # 9. LiveOps Tracked Events
    roll_count = data.get("roll_count", 0)
    pd_count = data.get("passdown_count", 0)
    total_liveops = roll_count + pd_count
    prior_liveops = data.get("prior_liveops_total")
    kpis.append({
        "label": "# Liveops Tracked Events",
        "value": str(total_liveops) if total_liveops > 0 else "—",
        "color": "" if total_liveops > 0 else "var(--dim)",
        "sub": f"{roll_count} Roll · {pd_count} Pass Down" if total_liveops > 0 else "Pending",
        "trend_text": trend_info_int(total_liveops, prior_liveops, higher_is_better=False)[0] if prior_liveops is not None else "No prior data",
        "trend_class": trend_info_int(total_liveops, prior_liveops, higher_is_better=False)[1] if prior_liveops is not None else "neutral",
    })
    
    return kpis


def render_dashboard(data: dict, output_path: Path) -> str:
    """Render the complete dashboard HTML.
    
    Args:
        data: Complete dashboard data dict
        output_path: Where to write the HTML file
    Returns:
        The rendered HTML string
    """
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=False,
    )
    template = env.get_template(TEMPLATE_NAME)
    
    html = template.render(**data)
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html)
    
    return html


def save_data_json(data: dict, output_dir: Path):
    """Save the raw data as JSON for next-day DoD comparison."""
    json_path = output_dir / "data.json"
    
    serializable = {}
    for key, val in data.items():
        if isinstance(val, date):
            serializable[key] = val.isoformat()
        elif isinstance(val, (list, dict, str, int, float, bool, type(None))):
            serializable[key] = val
    
    json_path.write_text(json.dumps(serializable, indent=2, default=str))


def load_prior_data(scorecard_dir: Path, op_date: date) -> dict:
    """Load the prior day's data.json for DoD comparison.
    
    Looks for the most recent data.json before op_date.
    """
    from datetime import timedelta
    
    for days_back in range(1, 8):
        prior_date = op_date - timedelta(days=days_back)
        prior_path = scorecard_dir / prior_date.isoformat() / "data.json"
        if prior_path.exists():
            try:
                return json.loads(prior_path.read_text())
            except (json.JSONDecodeError, OSError):
                continue
    return {}
