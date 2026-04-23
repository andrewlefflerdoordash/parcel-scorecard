#!/usr/bin/env python3
"""Find roles assigned to current user using alternative methods."""

import snowflake.connector

ACCOUNT = "doordash-dash_usw2"
USER = "andrew.leffler"
WAREHOUSE = "ADHOC"

conn = snowflake.connector.connect(
    account=ACCOUNT,
    user=USER,
    authenticator="externalbrowser",
    warehouse=WAREHOUSE,
)
cur = conn.cursor()

print("=== Current session info ===")
cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
print(cur.fetchone())

print("\n=== Trying SHOW GRANTS TO USER with email format ===")
user_variants = [
    'ANDREW.LEFFLER',
    '"andrew.leffler"',
    '"ANDREW.LEFFLER"',
    '"andrew.leffler@doordash.com"',
]
for uv in user_variants:
    try:
        cur.execute("SHOW GRANTS TO USER {}".format(uv))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        print("\n  Found grants for {}:".format(uv))
        for r in rows:
            d = dict(zip(cols, r))
            print("    role={}".format(d.get('role', d)))
        break
    except Exception as e:
        print("  {} failed: {}".format(uv, str(e)[:80]))

print("\n=== All databases visible with PUBLIC role ===")
cur.execute("USE ROLE PUBLIC")
cur.execute("SHOW DATABASES")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]
for r in rows:
    d = dict(zip(cols, r))
    print("  {}".format(d.get('name', '')))

print("\n=== Searching DERIVED database for forecast-related tables ===")
for db in ['DERIVED', 'RAW_GR']:
    try:
        cur.execute("USE DATABASE {}".format(db))
        cur.execute("SHOW SCHEMAS")
        schemas = cur.fetchall()
        s_cols = [d[0] for d in cur.description]
        schema_names = [dict(zip(s_cols, s)).get('name', '') for s in schemas]
        print("\n  {db} schemas ({n}): {schemas}".format(
            db=db, n=len(schema_names),
            schemas=', '.join(schema_names[:20])
        ))
        for sn in schema_names:
            if sn == 'INFORMATION_SCHEMA':
                continue
            try:
                cur.execute("SHOW TABLES LIKE '%PARCEL%' IN {}.{}".format(db, sn))
                tables = cur.fetchall()
                if tables:
                    t_cols = [d[0] for d in cur.description]
                    for t in tables:
                        td = dict(zip(t_cols, t))
                        print("    PARCEL table: {}.{}.{}".format(db, sn, td.get('name', '')))
            except:
                pass
            try:
                cur.execute("SHOW TABLES LIKE '%FORECAST%' IN {}.{}".format(db, sn))
                tables = cur.fetchall()
                if tables:
                    t_cols = [d[0] for d in cur.description]
                    for t in tables:
                        td = dict(zip(t_cols, t))
                        print("    FORECAST table: {}.{}.{}".format(db, sn, td.get('name', '')))
            except:
                pass
    except Exception as e:
        print("  {} error: {}".format(db, str(e)[:80]))

cur.close()
conn.close()
