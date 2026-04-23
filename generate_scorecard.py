#!/usr/bin/env python3
"""
Daily Parcel Scorecard Generator

Usage:
    python generate_scorecard.py                    # Auto-detect today, print collection instructions
    python generate_scorecard.py --date 2026-04-24  # Specific operational date
    python generate_scorecard.py --phase render      # Render from already-collected data
    python generate_scorecard.py --dry-run           # Render but don't commit/push
"""
import argparse
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from automation.config import (
    EXCLUDED_SITES, HUB_ROSTER, SPOKE_ROSTER, SIGMA_EMAILS,
    SLACK_ASK_PARCELS_CHANNEL, SLACK_TDD_CHANNEL,
    SHEETS_LIVEOPS_ID, SHEETS_OAUTH_CLIENT_ID, SHEETS_OAUTH_CLIENT_SECRET,
    SHEETS_REFRESH_TOKEN, OTD_MISS_THRESHOLD,
    get_op_date, get_data_date, format_date_long, format_date_short,
    format_date_mmdd, format_date_gen, format_date_iso, DAYS_OF_WEEK,
)
from automation.gmail_fetcher import (
    sigma_search_query, eos_search_query, decode_attachment,
    clean_csv_bytes, identify_hub_from_eos,
)
from automation.csv_parser import (
    parse_hub_forecasts, parse_hub_scanned, parse_cpt_metrics,
    parse_otd_misses, parse_spoke_forecast_scanned,
)
from automation.sheets_fetcher import (
    get_access_token, fetch_sheet_range, parse_roll_tracking,
    parse_pass_down, map_roll_to_sites, map_passdown_to_sites,
)
from automation.slack_fetcher import (
    ask_parcels_search_query, tdd_search_query,
    parse_ask_parcels_results, parse_tdd_results,
)
from automation.renderer import (
    build_kpi_tiles, render_dashboard, save_data_json,
    load_prior_data, format_number, compute_var_pct,
)
from automation.publisher import update_landing_page, git_commit_and_push


SCORECARD_DIR = Path(__file__).parent


def _build_ops_summary(
    otd_data, network_cpt, prior_data, hubs, spokes, tickets, tdd_callouts,
    hub_forecast_total, hub_scanned_total, spoke_forecast_total, spoke_scanned_total,
    roll_count, pd_count, data_date,
) -> str:
    """Auto-generate a concise operations summary narrative from the day's data."""
    parts = []
    ds = format_date_short(data_date)

    # OTD headline
    otd = otd_data.get("network_otd_pct")
    prior_otd = prior_data.get("network_otd_pct") if prior_data else None
    spoke_misses = otd_data.get("spoke_misses", {})
    above_thresh = sum(1 for v in spoke_misses.values() if v >= OTD_MISS_THRESHOLD)
    reporting = len(spoke_misses) if spoke_misses else len(spokes)
    if otd is not None:
        direction = "improved" if prior_otd and otd > prior_otd else "declined" if prior_otd and otd < prior_otd else "held"
        prior_str = f" ({direction} from {prior_otd}% on prior day)" if prior_otd else ""
        parts.append(
            f"Network OTD was <strong>{otd}%</strong>{prior_str}, "
            f"with {above_thresh} of {reporting} reporting spokes above the 0.35% miss attribution threshold."
        )

    # CPT highlights
    if network_cpt is not None:
        prior_cpt = prior_data.get("network_cpt_pct") if prior_data else None
        cpt_dir = ""
        if prior_cpt:
            cpt_dir = f" (down from {prior_cpt}%)" if network_cpt < prior_cpt else f" (up from {prior_cpt}%)"
        worst_hubs = sorted(
            [(h["name"], h["cpt_pct"]) for h in hubs if h["cpt_pct"] not in ("N/A", "—") and float(h["cpt_pct"].replace("%","")) < 85],
            key=lambda x: float(x[1].replace("%",""))
        )
        worst_str = ""
        if worst_hubs:
            worst_str = ", driven by " + ", ".join(f"{n} ({p})" for n, p in worst_hubs[:3])
        parts.append(f"Network Hub CPT was <strong>{network_cpt}%</strong>{cpt_dir}{worst_str}.")

    # Ticket highlights
    total_tickets = sum(len(v) for v in tickets.values())
    if total_tickets > 0:
        site_list = ", ".join(sorted(tickets.keys()))
        parts.append(
            f"<strong>{total_tickets} #ask-parcels tickets</strong> were filed across "
            f"{len(tickets)} site(s) ({site_list})."
        )

    # EOS highlights
    eos_hubs = [h for h in hubs if h["eos_status"] == "ok"]
    eos_with_notes = [h for h in eos_hubs if h["eos_notes"] and "&mdash;" not in h["eos_notes"]]
    if eos_with_notes:
        notable = []
        for h in eos_with_notes:
            if "rolled" in h["eos_notes"].lower() or "late" in h["eos_notes"].lower() or "miss" in h["eos_notes"].lower():
                notable.append(h["name"])
        if notable:
            parts.append(f"Notable EOS highlights from: {', '.join(notable)}.")

    # Hub forecast variance
    if hub_forecast_total and hub_scanned_total:
        var_pct = ((hub_scanned_total - hub_forecast_total) / hub_forecast_total) * 100
        parts.append(
            f"Hub forecast variance was <strong>{'+' if var_pct > 0 else ''}{var_pct:.1f}%</strong> "
            f"({format_number(hub_forecast_total)} forecast vs {format_number(hub_scanned_total)} scanned)."
        )

    # TDD callouts
    if tdd_callouts:
        tdd_sites = ", ".join(sorted(tdd_callouts.keys()))
        parts.append(f"TDD callouts flagged at: {tdd_sites}.")

    # LiveOps
    if roll_count or pd_count:
        parts.append(f"LiveOps tracked {roll_count} roll event(s) and {pd_count} pass-down note(s).")

    return "\n\n    ".join(parts) if parts else "No data available for summary generation."


def print_collection_instructions(op_date: date, data_date: date, data_dir: Path):
    """Print structured instructions for the AI agent to collect data."""
    print("=" * 70)
    print(f"SCORECARD DATA COLLECTION INSTRUCTIONS")
    print(f"Op Date: {format_date_long(op_date)} | Data Date: {format_date_short(data_date)}")
    print(f"Data Dir: {data_dir}")
    print("=" * 70)

    after_date = f"{data_date.year}/{data_date.month:02d}/{data_date.day:02d}"
    iso_date = format_date_iso(data_date)

    print(f"\n--- STEP 1: Sigma CSV Exports (Gmail) ---")
    for key, subject in SIGMA_EMAILS.items():
        query = sigma_search_query(subject, after_date)
        print(f"  [{key}] Search: {query}")
        print(f"    -> Save attachment CSV to: {data_dir}/{key}.csv")

    print(f"\n--- STEP 2: EOS Reports (Gmail) ---")
    query = eos_search_query(after_date)
    print(f"  Search: {query}")
    print(f"  -> Save parsed EOS data to: {data_dir}/eos_reports.json")
    print(f"  Format: {{\"hub_name\": {{\"status\": \"received\", \"notes\": \"...\"}} }}")

    print(f"\n--- STEP 3: #ask-parcels Tickets (Slack) ---")
    query = ask_parcels_search_query(data_date)
    print(f"  Search (user-slack): {query}")
    print(f"  -> Save raw results to: {data_dir}/ask_parcels_raw.txt")
    print(f"  -> Or save parsed data to: {data_dir}/ask_parcels.json")
    print(f"  Format: {{\"SITE-N\": [\"P1 — description\", ...] }}")

    print(f"\n--- STEP 4: TDD Callouts (Slack) ---")
    query = tdd_search_query(data_date)
    print(f"  Search (user-slack-dashlink-staples, channel {SLACK_TDD_CHANNEL}): {query}")
    print(f"  -> Save to: {data_dir}/tdd_callouts.json")
    print(f"  Format: {{\"SITE-N\": \"<span class=\\\"tag tag-tdd\\\">TDD</span> Category — description\" }}")

    print(f"\n--- STEP 5: Google Sheets (Roll Tracking + Pass Down) ---")
    print(f"  Sheet ID: {SHEETS_LIVEOPS_ID}")
    print(f"  Ranges: 'Roll Tracking!A1:Z500' and 'Pass Down!A1:Z500'")
    print(f"  -> Save to: {data_dir}/roll_tracking.json and {data_dir}/pass_down.json")

    print(f"\n--- STEP 6: Run Render ---")
    print(f"  python generate_scorecard.py --date {format_date_iso(op_date)} --phase render")
    print("=" * 70)


def collect_sheets_data(data_date: date, data_dir: Path):
    """Collect Google Sheets data (this CAN run from Python directly)."""
    print("  Fetching Google Sheets access token...")
    try:
        token = get_access_token(
            SHEETS_OAUTH_CLIENT_ID, SHEETS_OAUTH_CLIENT_SECRET, SHEETS_REFRESH_TOKEN
        )
    except Exception as e:
        print(f"  ERROR getting Sheets token: {e}")
        return

    print("  Fetching Roll Tracking...")
    try:
        roll_rows = fetch_sheet_range(token, SHEETS_LIVEOPS_ID, "Roll Tracking!A1:Z500")
        entries = parse_roll_tracking(roll_rows, data_date)
        (data_dir / "roll_tracking.json").write_text(json.dumps(entries, indent=2))
        print(f"    Found {len(entries)} roll tracking entries for {format_date_short(data_date)}")
    except Exception as e:
        print(f"    ERROR: {e}")

    print("  Fetching Pass Down...")
    try:
        pass_rows = fetch_sheet_range(token, SHEETS_LIVEOPS_ID, "Pass Down!A1:Z500")
        entries = parse_pass_down(pass_rows, data_date)
        (data_dir / "pass_down.json").write_text(json.dumps(entries, indent=2))
        print(f"    Found {len(entries)} pass down entries for {format_date_short(data_date)}")
    except Exception as e:
        print(f"    ERROR: {e}")


def render_from_data(op_date: date, data_date: date, data_dir: Path, dry_run: bool = False):
    """Render the dashboard from collected data files."""
    print(f"\n--- RENDERING DASHBOARD ---")
    print(f"  Op Date: {format_date_long(op_date)}")
    print(f"  Data Date: {format_date_short(data_date)}")

    prior_data = load_prior_data(SCORECARD_DIR, op_date)

    target_iso = format_date_iso(data_date)

    hub_forecasts = {}
    csv_path = data_dir / "hub_forecasts.csv"
    if csv_path.exists():
        hub_forecasts = parse_hub_forecasts(csv_path.read_text(errors='replace'), target_iso)
        print(f"  Hub forecasts: {len(hub_forecasts)} sites")

    hub_scanned = {}
    csv_path = data_dir / "hub_scanned.csv"
    if csv_path.exists():
        hub_scanned = parse_hub_scanned(csv_path.read_text(errors='replace'), target_iso)
        print(f"  Hub scanned: {len(hub_scanned)} sites")

    cpt_data = {}
    csv_path = data_dir / "cpt_metrics.csv"
    if csv_path.exists():
        cpt_data = parse_cpt_metrics(csv_path.read_text(errors='replace'), target_iso)
        print(f"  CPT metrics: {len(cpt_data)} sites")

    otd_data = {"network_otd_pct": None, "spoke_misses": {}}
    csv_path = data_dir / "otd_misses.csv"
    if csv_path.exists():
        otd_data = parse_otd_misses(csv_path.read_text(errors='replace'), target_iso)
        print(f"  OTD data: network={otd_data.get('network_otd_pct')}%, {len(otd_data.get('spoke_misses', {}))} spokes")

    spoke_data = {"forecast": {}, "scanned": {}}
    csv_path = data_dir / "spoke_forecast_scanned.csv"
    if csv_path.exists():
        spoke_data = parse_spoke_forecast_scanned(csv_path.read_text(errors='replace'), target_iso)
        print(f"  Spoke forecast: {len(spoke_data['forecast'])} sites, scanned: {len(spoke_data['scanned'])} sites")

    eos_reports = {}
    eos_path = data_dir / "eos_reports.json"
    if eos_path.exists():
        eos_reports = json.loads(eos_path.read_text())
        print(f"  EOS reports: {len(eos_reports)} hubs")

    tickets = {}
    tickets_path = data_dir / "ask_parcels.json"
    if tickets_path.exists():
        tickets = json.loads(tickets_path.read_text())
        print(f"  Tickets: {sum(len(v) for v in tickets.values())} across {len(tickets)} sites")

    tdd_callouts = {}
    tdd_path = data_dir / "tdd_callouts.json"
    if tdd_path.exists():
        tdd_callouts = json.loads(tdd_path.read_text())
        print(f"  TDD callouts: {len(tdd_callouts)} sites")

    roll_liveops = {}
    roll_path = data_dir / "roll_tracking.json"
    if roll_path.exists():
        roll_entries = json.loads(roll_path.read_text())
        roll_liveops = map_roll_to_sites(roll_entries)
        print(f"  Roll tracking: {len(roll_entries)} entries -> {len(roll_liveops)} sites")

    passdown_liveops = {}
    pd_path = data_dir / "pass_down.json"
    if pd_path.exists():
        pd_entries = json.loads(pd_path.read_text())
        passdown_liveops = map_passdown_to_sites(pd_entries)
        print(f"  Pass down: {len(pd_entries)} entries -> {len(passdown_liveops)} sites")

    all_liveops = {}
    for site, entries in roll_liveops.items():
        all_liveops.setdefault(site, []).extend(entries)
    for site, entries in passdown_liveops.items():
        all_liveops.setdefault(site, []).extend(entries)

    hubs = []
    for site_name in sorted(HUB_ROSTER.keys()):
        if site_name in EXCLUDED_SITES:
            continue
        partner = HUB_ROSTER[site_name]
        fv = hub_forecasts.get(site_name)
        sv = hub_scanned.get(site_name)
        var_str, var_color = compute_var_pct(fv, sv)

        cpt_val = cpt_data.get(site_name)
        if cpt_val is not None:
            cpt_str = f"{cpt_val:.1f}%"
            cpt_color = "var(--green)" if cpt_val >= 95 else "var(--yellow)" if cpt_val >= 80 else "var(--red)"
        else:
            cpt_str = "N/A"
            cpt_color = "var(--dim)"

        eos_info = eos_reports.get(site_name, {})
        eos_status = "ok" if eos_info.get("status") == "received" else "miss"
        eos_notes = eos_info.get("notes", '<span class="pending-text">&mdash;</span>')

        site_tickets = tickets.get(site_name, [])
        ticket_html = "<br>".join(site_tickets) if site_tickets else "-"

        site_liveops = all_liveops.get(site_name, [])
        liveops_html = "<br>".join(site_liveops) if site_liveops else "-"

        hubs.append({
            "name": site_name,
            "sort_partner": partner,
            "forecast_vol": format_number(fv),
            "scanned_vol": format_number(sv),
            "var_pct": var_str,
            "var_color": var_color,
            "cpt_pct": cpt_str,
            "cpt_color": cpt_color,
            "eos_status": eos_status,
            "eos_notes": eos_notes,
            "tickets": ticket_html,
            "liveops": liveops_html,
            "escalation": False,
            "tags": [],
        })

    spoke_misses = otd_data.get("spoke_misses", {})
    spokes_above = 0
    spokes_below = 0
    total_reporting = 0

    spokes = []
    for site_name in sorted(SPOKE_ROSTER.keys()):
        if site_name in EXCLUDED_SITES:
            continue
        partner = SPOKE_ROSTER[site_name]
        fv = spoke_data["forecast"].get(site_name)
        sv = spoke_data["scanned"].get(site_name)
        var_str, var_color = compute_var_pct(fv, sv)

        miss_pct = spoke_misses.get(site_name, 0.0)
        total_reporting += 1
        if miss_pct >= OTD_MISS_THRESHOLD:
            spokes_above += 1
            otd_color = "var(--red)"
        else:
            spokes_below += 1
            otd_color = "var(--green)"
        otd_str = f"{miss_pct:.4f}%" if miss_pct > 0 else "0%"

        site_tickets = tickets.get(site_name, [])
        ticket_html = "<br>".join(site_tickets) if site_tickets else "-"

        tdd_html = tdd_callouts.get(site_name, "-")

        site_liveops = all_liveops.get(site_name, [])
        if site_liveops:
            liveops_html = "<br>".join(site_liveops)
        else:
            liveops_html = '<span style="font-style:italic; color:var(--green)">No Issues Tracked</span>'

        spokes.append({
            "name": site_name,
            "sort_partner": partner,
            "forecast_vol": format_number(fv),
            "scanned_vol": format_number(sv),
            "var_pct": var_str,
            "var_color": var_color,
            "otd_miss_pct": otd_str,
            "otd_miss_color": otd_color,
            "tickets": ticket_html,
            "tdd_callouts": tdd_html,
            "liveops": liveops_html,
        })

    hub_forecast_total = sum(v for v in hub_forecasts.values() if v)
    hub_scanned_total = sum(v for v in hub_scanned.values() if v)
    spoke_forecast_total = sum(v for v in spoke_data["forecast"].values() if v)
    spoke_scanned_total = sum(v for v in spoke_data["scanned"].values() if v)

    network_cpt = cpt_data.get("Network")

    eos_received = [h["name"] for h in hubs if h["eos_status"] == "ok"]

    total_tickets = sum(len(v) for v in tickets.values())
    ticket_sites = len(tickets)

    roll_count = len(json.loads((data_dir / "roll_tracking.json").read_text())) if (data_dir / "roll_tracking.json").exists() else 0
    pd_count = len(json.loads((data_dir / "pass_down.json").read_text())) if (data_dir / "pass_down.json").exists() else 0

    sources = []
    sources.append({"label": "✓ OTD (Sigma)", "status": "active" if otd_data.get("network_otd_pct") else "na"})
    sources.append({"label": "✓ #ask-parcels", "status": "active" if tickets else "na"})
    sources.append({"label": f"✓ TDD DashLink ({len(tdd_callouts)} callouts)", "status": "active" if tdd_callouts else "na"})
    sources.append({"label": f"✓ Gmail EOS ({format_date_short(data_date)})", "status": "active" if eos_reports else "warn"})
    sources.append({"label": "✓ Google Sheets (Roll/Pass Down)", "status": "active" if roll_count or pd_count else "na"})
    sources.append({"label": "✓ Sigma Exports", "status": "active"})

    data_dict = {
        "op_date_long": format_date_long(op_date),
        "data_date_short": format_date_short(data_date),
        "op_date_short": format_date_mmdd(op_date),
        "op_date_day": DAYS_OF_WEEK[op_date.weekday()],
        "generated_date": format_date_gen(op_date),
        "hub_count": len(hubs),
        "spoke_count": len(spokes),
        "sources": sources,
        "ops_summary": _build_ops_summary(
            otd_data, network_cpt, prior_data, hubs, spokes, tickets, tdd_callouts,
            hub_forecast_total, hub_scanned_total, spoke_forecast_total, spoke_scanned_total,
            roll_count, pd_count, data_date,
        ),
        "hubs": hubs,
        "spokes": spokes,
        "network_otd_pct": otd_data.get("network_otd_pct"),
        "network_cpt_pct": network_cpt,
        "hub_forecast_total": hub_forecast_total,
        "hub_scanned_total": hub_scanned_total,
        "spoke_forecast_total": spoke_forecast_total,
        "spoke_scanned_total": spoke_scanned_total,
        "spokes_above_threshold": spokes_above,
        "spokes_below_threshold": spokes_below,
        "total_reporting_spokes": total_reporting,
        "eos_received_count": len(eos_received),
        "eos_total_count": len(hubs),
        "eos_hub_names": eos_received,
        "ticket_site_count": ticket_sites,
        "ticket_total": total_tickets,
        "ticket_breakdown": f"{total_tickets} P1" if total_tickets else "",
        "roll_count": roll_count,
        "passdown_count": pd_count,
    }

    if prior_data:
        data_dict["prior_network_otd_pct"] = prior_data.get("network_otd_pct")
        data_dict["prior_network_cpt_pct"] = prior_data.get("network_cpt_pct")
        data_dict["prior_spokes_above_threshold"] = prior_data.get("spokes_above_threshold")
        data_dict["prior_spokes_below_threshold"] = prior_data.get("spokes_below_threshold")
        data_dict["prior_eos_received"] = prior_data.get("eos_received_count")
        data_dict["prior_ticket_site_count"] = prior_data.get("ticket_site_count")
        data_dict["prior_liveops_total"] = (prior_data.get("roll_count", 0) + prior_data.get("passdown_count", 0)) or None
        prior_hf = prior_data.get("hub_forecast_total", 0)
        prior_hs = prior_data.get("hub_scanned_total", 0)
        data_dict["prior_hub_forecast_var"] = ((prior_hs - prior_hf) / prior_hf * 100) if prior_hf else None
        prior_sf = prior_data.get("spoke_forecast_total", 0)
        prior_ss = prior_data.get("spoke_scanned_total", 0)
        data_dict["prior_spoke_forecast_var"] = ((prior_ss - prior_sf) / prior_sf * 100) if prior_sf else None

    data_dict["kpis"] = build_kpi_tiles(data_dict)

    tdd_sites = ", ".join(tdd_callouts.keys()) if tdd_callouts else "none"
    data_dict["footer_text"] = (
        f"Parcel Core Ops Daily Site Review &nbsp;•&nbsp; "
        f"Op Day {format_date_mmdd(op_date)} ({DAYS_OF_WEEK[op_date.weekday()]}) &nbsp;•&nbsp; "
        f"Sources: Sigma Exports (Hub Vol, CPT, OTD, Spoke Vol) · "
        f"Gmail EOS ({len(eos_received)}/{len(hubs)}) · "
        f"#ask-parcels ({total_tickets} tickets, {ticket_sites} sites) · "
        f"Google Sheets ({roll_count} Roll · {pd_count} Pass Down) · "
        f"TDD DashLink ({len(tdd_callouts)} callouts: {tdd_sites})"
    )

    output_dir = SCORECARD_DIR / format_date_iso(op_date)
    output_path = output_dir / "index.html"

    print(f"\n  Rendering to {output_path}...")
    render_dashboard(data_dict, output_path)
    save_data_json(data_dict, output_dir)
    print(f"  Dashboard saved!")

    if not dry_run:
        print(f"\n  Updating landing page...")
        update_landing_page(SCORECARD_DIR, op_date, len(hubs), len(spokes))

        summary = (
            f"Network OTD: {otd_data.get('network_otd_pct', 'N/A')}%, "
            f"Hub CPT: {network_cpt or 'N/A'}%, "
            f"EOS: {len(eos_received)}/{len(hubs)}, "
            f"Tickets: {total_tickets}, "
            f"LiveOps: {roll_count}R/{pd_count}PD"
        )
        print(f"  Committing and pushing...")
        git_commit_and_push(SCORECARD_DIR, op_date, summary)
        print(f"  Published!")
    else:
        print(f"  DRY RUN - skipping commit/push")

    print(f"\n  DONE!")
    return data_dict


def main():
    parser = argparse.ArgumentParser(description="Generate daily parcel scorecard")
    parser.add_argument("--date", help="Operational date (YYYY-MM-DD), defaults to today")
    parser.add_argument("--phase", choices=["collect", "render", "sheets"], default="collect",
                       help="Phase to run: collect (print instructions), render, or sheets (auto-fetch)")
    parser.add_argument("--dry-run", action="store_true", help="Don't commit/push")
    args = parser.parse_args()

    op_date = get_op_date(args.date)
    data_date = get_data_date(op_date)
    data_dir = SCORECARD_DIR / format_date_iso(op_date) / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    if args.phase == "collect":
        print_collection_instructions(op_date, data_date, data_dir)
    elif args.phase == "sheets":
        collect_sheets_data(data_date, data_dir)
    elif args.phase == "render":
        render_from_data(op_date, data_date, data_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
