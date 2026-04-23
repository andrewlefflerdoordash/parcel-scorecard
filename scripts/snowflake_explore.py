#!/usr/bin/env python3
"""Explore available Snowflake databases and find the forecast table."""

import snowflake.connector

ACCOUNT = "doordash-dash_usw2"
USER = "andrew.leffler"
WAREHOUSE = "ADHOC"

def connect():
    return snowflake.connector.connect(
        account=ACCOUNT,
        user=USER,
        authenticator="externalbrowser",
        warehouse=WAREHOUSE,
    )

conn = connect()
cur = conn.cursor()

print("=== Current context ===")
cur.execute("SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
print(cur.fetchone())

print("\n=== Searching for databases containing 'edw' ===")
cur.execute("SHOW DATABASES LIKE '%edw%'")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]
for r in rows[:20]:
    d = dict(zip(cols, r))
    print("  {} (origin: {})".format(d.get('name', ''), d.get('origin', '')))

if not rows:
    print("  (none found, listing all databases)")
    cur.execute("SHOW DATABASES")
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    for r in rows[:30]:
        d = dict(zip(cols, r))
        print("  {}".format(d.get('name', '')))

print("\n=== Trying to find forecast table directly ===")
try:
    cur.execute("""
        SELECT table_catalog, table_schema, table_name 
        FROM information_schema.tables 
        WHERE LOWER(table_name) LIKE '%forecast_report_snapshot%'
        LIMIT 10
    """)
    for row in cur.fetchall():
        print("  Found: {}.{}.{}".format(*row))
except Exception as e:
    print("  info_schema search failed: {}".format(e))

try:
    cur.execute("SHOW TABLES LIKE '%FORECAST_REPORT_SNAPSHOT%' IN ACCOUNT")
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    for r in rows[:10]:
        d = dict(zip(cols, r))
        print("  Found: {}.{}.{}".format(d.get('database_name', ''), d.get('schema_name', ''), d.get('name', '')))
except Exception as e:
    print("  account-wide search failed: {}".format(e))

cur.close()
conn.close()
