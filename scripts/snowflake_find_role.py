#!/usr/bin/env python3
"""Try different roles to find one that can access EDW.DRIVE."""

import snowflake.connector

ACCOUNT = "doordash-dash_usw2"
USER = "andrew.leffler"
WAREHOUSE = "ADHOC"

CANDIDATE_ROLES = [
    "TEAM-DRIVE-DE",
    "TEAM-DRIVE-CORE",
    "TEAM-DRIVE-PARCEL-RUNNER",
    "TEAM-PARCEL-WAREHOUSE-OPS",
    "TEAM-PARCEL-AND-RETAIL-GROWTH",
    "TEAM-DRIVE-DS-AX",
    "TEAM-DRIVE-PLATFORM",
    "TEAM-OPERATIONS-DE",
    "DOMAIN_LOGISTICS_DERIVED_READERS",
    "DOMAIN_LOGISTICS_RAW_READERS",
    "TEAM-SHIPPING",
    "SYSADMIN",
]

conn = snowflake.connector.connect(
    account=ACCOUNT,
    user=USER,
    authenticator="externalbrowser",
    warehouse=WAREHOUSE,
)
cur = conn.cursor()

for role in CANDIDATE_ROLES:
    try:
        cur.execute('USE ROLE "{}"'.format(role))
        cur.execute("SHOW DATABASES LIKE 'EDW'")
        dbs = cur.fetchall()
        if dbs:
            print("FOUND EDW with role: {}".format(role))
            cur.execute("SHOW SCHEMAS IN DATABASE EDW LIKE 'DRIVE'")
            schemas = cur.fetchall()
            if schemas:
                print("  EDW.DRIVE schema exists!")
                cur.execute("SHOW TABLES LIKE '%FORECAST_REPORT_SNAPSHOT%' IN EDW.DRIVE")
                tables = cur.fetchall()
                if tables:
                    t_cols = [d[0] for d in cur.description]
                    for t in tables:
                        td = dict(zip(t_cols, t))
                        print("  Table found: {}".format(td.get('name', '')))

                    cur.execute("""
                        SELECT * FROM EDW.DRIVE.GSHEET_PARCEL_STAPLES_FORECAST_REPORT_SNAPSHOT
                        WHERE DAY = '2026-04-21'
                        LIMIT 5
                    """)
                    sample = cur.fetchall()
                    cols = [d[0] for d in cur.description]
                    print("\n  Columns: {}".format(cols))
                    print("  Sample rows for 2026-04-21:")
                    for row in sample:
                        print("    {}".format(dict(zip(cols, row))))
                    break
                else:
                    print("  Table not found in EDW.DRIVE")
            else:
                print("  DRIVE schema not found in EDW")
        else:
            pass
    except Exception as e:
        err_msg = str(e)
        if "is not authorized" not in err_msg and "does not exist" not in err_msg:
            print("Role {} error: {}".format(role, err_msg[:120]))
else:
    print("No candidate role found EDW.DRIVE.GSHEET_PARCEL_STAPLES_FORECAST_REPORT_SNAPSHOT")

cur.close()
conn.close()
