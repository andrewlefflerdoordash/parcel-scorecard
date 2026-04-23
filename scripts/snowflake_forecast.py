#!/usr/bin/env python3
"""
Pull hub & spoke forecast volumes from Snowflake for the parcel scorecard.
Uses SSO (externalbrowser) auth -- will open a browser window on first run.
"""

import sys
import json
import snowflake.connector

ACCOUNT = "doordash-dash_usw2"
USER = "andrew.leffler"
WAREHOUSE = "ADHOC"
DATABASE = "EDW"
SCHEMA = "DRIVE"
TABLE = "GSHEET_PARCEL_STAPLES_FORECAST_REPORT_SNAPSHOT"

def connect():
    return snowflake.connector.connect(
        account=ACCOUNT,
        user=USER,
        authenticator="externalbrowser",
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
    )

def fetch_forecast(conn, target_date):
    """Fetch forecast data for a given date (YYYY-MM-DD)."""
    query = """
        SELECT DAY, SITE, HUB, VOLUME
        FROM {db}.{schema}.{table}
        WHERE DAY = %s
        ORDER BY HUB, SITE
    """.format(db=DATABASE, schema=SCHEMA, table=TABLE)

    cur = conn.cursor()
    cur.execute(query, (target_date,))
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    cur.close()
    return [dict(zip(cols, row)) for row in rows]

if __name__ == "__main__":
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-04-21"

    print("Connecting to Snowflake via SSO (a browser window will open)...")
    conn = connect()
    print("Connected successfully!\n")

    print("Fetching forecast data for {}...".format(target_date))
    data = fetch_forecast(conn, target_date)
    print("Retrieved {} rows\n".format(len(data)))

    if data:
        print("--- Sample (first 10 rows) ---")
        for row in data[:10]:
            print("  {SITE:20s}  Hub: {HUB:10s}  Volume: {VOLUME}".format(**row))
        if len(data) > 10:
            print("  ... and {} more rows".format(len(data) - 10))

    print("\n--- Full JSON output ---")
    print(json.dumps(data, default=str, indent=2))

    conn.close()
