#!/usr/bin/env python3
"""
Pull spoke sortation OTD miss attribution from Snowflake and update the
parcel-scorecard HTML.

Source: proddb.static.parcel_golden_path_otd_barcodes
Method: Spoke Sortation miss barcodes / total network barcodes for the day

Usage:
    python pull_otd_attribution.py                   # auto-detect latest scorecard date
    python pull_otd_attribution.py 2026-04-22        # specific date folder
    python pull_otd_attribution.py --dry-run          # print results without updating HTML
"""

import argparse
import json
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
MISS_THRESHOLD = 0.35  # spoke miss % threshold for KPI cards


def connect_snowflake():
    import snowflake.connector
    return snowflake.connector.connect(
        account="DOORDASH-DOORDASH",
        user="ANDREW.LEFFLER",
        authenticator="externalbrowser",
        role="ANDREWLEFFLER",
        warehouse="ADHOC",
        database="PRODDB",
        schema="STATIC",
        client_store_temporary_credential=True,
        login_timeout=120,
    )


def pull_otd_data(conn, tdd: date):
    """Pull spoke sortation OTD miss attribution for a target delivery date."""
    cur = conn.cursor()
    tdd_str = tdd.isoformat()

    # Network OTD
    cur.execute(f"""
        SELECT
            COUNT(DISTINCT CASE WHEN OTD = '1' THEN BARCODE END) AS otd_barcodes,
            COUNT(DISTINCT CASE WHEN OTD = '0' THEN BARCODE END) AS miss_barcodes,
            COUNT(DISTINCT BARCODE) AS total_barcodes
        FROM proddb.static.parcel_golden_path_otd_barcodes
        WHERE TARGET_DELIVERY_DATE = '{tdd_str}'
    """)
    row = cur.fetchone()
    if not row or row[2] == 0:
        print(f"  No data found for TARGET_DELIVERY_DATE = {tdd_str}")
        return None

    network = {
        "otd_barcodes": row[0],
        "miss_barcodes": row[1],
        "total_barcodes": row[2],
        "otd_pct": round(row[0] * 100.0 / row[2], 2),
    }
    print(f"  Network OTD: {network['otd_pct']}% ({network['otd_barcodes']:,} / {network['total_barcodes']:,})")

    # Spoke sortation miss attribution by facility
    cur.execute(f"""
        SELECT
            FACILITY_OR_ROUTE AS site,
            COUNT(DISTINCT BARCODE) AS miss_barcodes
        FROM proddb.static.parcel_golden_path_otd_barcodes
        WHERE TARGET_DELIVERY_DATE = '{tdd_str}'
          AND IS_OTD_MISS = 'OTD Miss Step'
          AND STEP_TYPE_BUCKET = '4. Spoke Sortation'
        GROUP BY FACILITY_OR_ROUTE
        ORDER BY miss_barcodes DESC
    """)
    spoke_misses = {}
    for row in cur.fetchall():
        site, miss_count = row[0], row[1]
        pct = round(miss_count * 100.0 / network["total_barcodes"], 4)
        spoke_misses[site] = {"miss_barcodes": miss_count, "miss_pct": pct}

    cur.close()
    return {"network": network, "spoke_misses": spoke_misses}


def format_miss_pct(pct: float) -> str:
    if pct == 0:
        return "0%"
    return f"{pct:.4f}%"


def build_otd_cell(pct: float) -> str:
    color = "var(--red)" if pct >= MISS_THRESHOLD else "var(--green)"
    display = format_miss_pct(pct)
    return f'<td style="text-align:center"><span style="color:{color}">{display}</span></td>'


def update_html(html_path: Path, data: dict) -> str:
    html = html_path.read_text()
    total = data["network"]["total_barcodes"]
    spoke_misses = data["spoke_misses"]

    # Find all spoke rows and update OTD Miss Attrib column
    site_pattern = re.compile(
        r'(<td class="site-col">)([A-Z]{2,5}-\d{1,2})(</td>)'
        r'(.*?)'  # rest of row through sort partner, forecast, scanned, %var
        r'(<td[^>]*>)([^<]*(?:<[^>]*>[^<]*)*)(</td>)'  # OTD Miss Attrib cell (6th td)
        r'(.*?</tr>)',
        re.DOTALL,
    )

    # Simpler approach: replace each spoke's pending OTD cell
    updated_sites = []
    for site_name, miss_data in spoke_misses.items():
        updated_sites.append(site_name)

    # Parse and replace each spoke row's OTD cell
    lines = html.split("\n")
    new_lines = []
    spokes_above_threshold = 0
    spokes_below_threshold = 0
    total_spokes_updated = 0

    in_spoke_section = False
    for line in lines:
        # Detect spoke section
        if "🚗 Spokes" in line:
            in_spoke_section = True
        if "🚛 Hubs" in line:
            in_spoke_section = False

        if in_spoke_section and '<td class="site-col">' in line:
            site_match = re.search(r'<td class="site-col">([^<]+)</td>', line)
            if site_match:
                site_name = site_match.group(1)
                pct = spoke_misses.get(site_name, {}).get("miss_pct", 0.0)

                if pct >= MISS_THRESHOLD:
                    spokes_above_threshold += 1
                else:
                    spokes_below_threshold += 1
                total_spokes_updated += 1

                new_cell = build_otd_cell(pct)
                # Replace the pending OTD cell (6th <td> in the row)
                # Split by </td> to find the 6th cell
                parts = line.split("</td>")
                if len(parts) >= 6:
                    # The 6th cell (index 5) is OTD Miss Attrib
                    old_cell = parts[5] + "</td>"
                    parts[5] = new_cell.rstrip("</td>").rstrip("d>").rstrip("</t")
                    # Reconstruct: replace 6th <td>...</td> segment
                    # Easier: use regex on the line
                    td_pattern = re.compile(r'(<td[^>]*>[^<]*(?:<[^>]*>[^<]*)*</td>)')
                    cells = td_pattern.findall(line)
                    if len(cells) >= 6:
                        old_otd_cell = cells[5]
                        line = line.replace(old_otd_cell, new_cell, 1)

        new_lines.append(line)

    html = "\n".join(new_lines)

    # Update KPI cards: # Spokes w/OTD Miss > 0.35%
    html = re.sub(
        r'(# Spokes w/OTD Miss &gt; 0\.35%</div><div class="val"[^>]*>)\d+',
        rf"\g<1>{spokes_above_threshold}",
        html,
    )

    # Update KPI: # Clean Spokes w/OTD Miss < 0.35%
    html = re.sub(
        r'(# Clean Spokes w/OTD Miss &lt; 0\.35%</div><div class="val"[^>]*>)\d+',
        rf"\g<1>{spokes_below_threshold}",
        html,
    )

    # Update KPI: Network OTD %
    otd_pct = data["network"]["otd_pct"]
    otd_color = "var(--green)" if otd_pct >= 95 else "var(--yellow)" if otd_pct >= 90 else "var(--red)"
    html = re.sub(
        r'(Network OTD %</div><div class="val" style="color:)[^"]*(">[^<]*<)',
        rf'\g<1>{otd_color}\2',
        html,
    )
    html = re.sub(
        r'(Network OTD %</div><div class="val" style="color:[^"]*">)[0-9.]+%',
        rf"\g<1>{otd_pct}%",
        html,
    )

    # Update source badge: OTD (CSV) → OTD (Snowflake)
    html = html.replace("✓ OTD (CSV)", "✓ OTD (Snowflake)")

    # Update footer source
    html = html.replace("OTD CSV", "OTD Snowflake")

    print(f"  Updated {total_spokes_updated} spoke rows")
    print(f"  Spokes above 0.35%: {spokes_above_threshold}")
    print(f"  Clean spokes: {spokes_below_threshold}")

    return html


def detect_latest_date_folder() -> Path | None:
    folders = sorted(SCRIPT_DIR.glob("2026-*"), reverse=True)
    for f in folders:
        if f.is_dir() and (f / "index.html").exists():
            return f
    return None


def infer_tdd_from_folder(folder_name: str) -> date:
    """The folder is the op day; TDD is the day before (data day)."""
    op_date = datetime.strptime(folder_name, "%Y-%m-%d").date()
    return op_date - timedelta(days=1)


def main():
    parser = argparse.ArgumentParser(description="Pull OTD attribution from Snowflake")
    parser.add_argument("date_folder", nargs="?", help="Date folder name (e.g. 2026-04-22)")
    parser.add_argument("--dry-run", action="store_true", help="Print results without updating HTML")
    parser.add_argument("--output-json", help="Also save raw data to a JSON file")
    args = parser.parse_args()

    if args.date_folder:
        target_dir = SCRIPT_DIR / args.date_folder
    else:
        target_dir = detect_latest_date_folder()
        if not target_dir:
            print("No date folders found. Specify a date folder.")
            sys.exit(1)

    html_path = target_dir / "index.html"
    if not html_path.exists():
        print(f"No index.html found in {target_dir}")
        sys.exit(1)

    folder_name = target_dir.name
    tdd = infer_tdd_from_folder(folder_name)
    print(f"Scorecard folder: {folder_name}")
    print(f"Target delivery date (TDD): {tdd}")
    print(f"Connecting to Snowflake...")

    conn = connect_snowflake()
    print(f"Pulling OTD data...")
    data = pull_otd_data(conn, tdd)
    conn.close()

    if not data:
        print("No data returned. Exiting.")
        sys.exit(1)

    if args.output_json:
        json_path = Path(args.output_json)
        serializable = {
            "target_delivery_date": tdd.isoformat(),
            "folder": folder_name,
            "network": data["network"],
            "spoke_misses": {
                k: {"miss_barcodes": v["miss_barcodes"], "miss_pct": v["miss_pct"]}
                for k, v in data["spoke_misses"].items()
            },
        }
        json_path.write_text(json.dumps(serializable, indent=2))
        print(f"  Saved JSON to {json_path}")

    if args.dry_run:
        print("\n--- DRY RUN (no HTML changes) ---")
        print(f"\nNetwork OTD: {data['network']['otd_pct']}%")
        print(f"Total barcodes: {data['network']['total_barcodes']:,}")
        print(f"Miss barcodes: {data['network']['miss_barcodes']:,}")
        print(f"\nSpoke Sortation Miss Attribution:")
        for site, d in sorted(data["spoke_misses"].items(), key=lambda x: -x[1]["miss_pct"]):
            flag = " ⚠" if d["miss_pct"] >= MISS_THRESHOLD else ""
            print(f"  {site:<12} {d['miss_pct']:.4f}%  ({d['miss_barcodes']} barcodes){flag}")
        return

    print(f"Updating {html_path}...")
    updated_html = update_html(html_path, data)
    html_path.write_text(updated_html)
    print(f"Done! Updated {html_path}")


if __name__ == "__main__":
    main()
