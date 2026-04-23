#!/usr/bin/env python3
"""Search for the forecast table across available databases and roles."""

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

print("=== Available roles ===")
cur.execute("SHOW ROLES")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]
for r in rows:
    d = dict(zip(cols, r))
    print("  {}".format(d.get('name', '')))

print("\n=== Trying different roles to find EDW database ===")
for r in rows:
    role_name = dict(zip(cols, r)).get('name', '')
    if role_name in ('PUBLIC', 'SNOWFLAKE'):
        continue
    try:
        cur.execute("USE ROLE {}".format(role_name))
        cur.execute("SHOW DATABASES LIKE '%EDW%'")
        dbs = cur.fetchall()
        if dbs:
            db_cols = [d[0] for d in cur.description]
            for db in dbs:
                dd = dict(zip(db_cols, db))
                print("  Role '{}' can see: {}".format(role_name, dd.get('name', '')))
    except Exception as e:
        pass

print("\n=== Searching DERIVED for forecast table ===")
try:
    cur.execute("USE ROLE PUBLIC")
    cur.execute("SHOW SCHEMAS IN DATABASE DERIVED")
    schemas = cur.fetchall()
    s_cols = [d[0] for d in cur.description]
    for s in schemas:
        sd = dict(zip(s_cols, s))
        sname = sd.get('name', '')
        if sname.lower() in ('information_schema',):
            continue
        try:
            cur.execute("SHOW TABLES LIKE '%FORECAST%' IN DERIVED.{}".format(sname))
            tables = cur.fetchall()
            if tables:
                t_cols = [d[0] for d in cur.description]
                for t in tables:
                    td = dict(zip(t_cols, t))
                    print("  Found: DERIVED.{}.{}".format(sname, td.get('name', '')))
        except:
            pass
except Exception as e:
    print("  Error: {}".format(e))

print("\n=== Searching RAW_GR for forecast table ===")
try:
    cur.execute("SHOW SCHEMAS IN DATABASE RAW_GR")
    schemas = cur.fetchall()
    s_cols = [d[0] for d in cur.description]
    for s in schemas:
        sd = dict(zip(s_cols, s))
        sname = sd.get('name', '')
        if sname.lower() in ('information_schema',):
            continue
        try:
            cur.execute("SHOW TABLES LIKE '%FORECAST%' IN RAW_GR.{}".format(sname))
            tables = cur.fetchall()
            if tables:
                t_cols = [d[0] for d in cur.description]
                for t in tables:
                    td = dict(zip(t_cols, t))
                    print("  Found: RAW_GR.{}.{}".format(sname, td.get('name', '')))
        except:
            pass
except Exception as e:
    print("  Error: {}".format(e))

cur.close()
conn.close()
