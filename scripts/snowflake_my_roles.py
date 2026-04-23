#!/usr/bin/env python3
"""Find roles assigned to current user and check EDW access."""

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

print("=== Roles granted to current user ===")
cur.execute("SHOW GRANTS TO USER ANDREW_LEFFLER")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]
my_roles = []
for r in rows:
    d = dict(zip(cols, r))
    role_name = d.get('role', d.get('name', ''))
    print("  {}".format(d))
    my_roles.append(role_name)

if not my_roles:
    cur.execute("SELECT CURRENT_ROLE()")
    current = cur.fetchone()[0]
    print("\n  Current role: {}".format(current))
    print("\n  Trying SHOW GRANTS TO USER \"andrew.leffler\"")
    try:
        cur.execute('SHOW GRANTS TO USER "andrew.leffler"')
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        for r in rows:
            d = dict(zip(cols, r))
            print("  {}".format(d))
            my_roles.append(d.get('role', d.get('name', '')))
    except Exception as e:
        print("  Error: {}".format(e))

print("\n=== Trying each role for EDW access ===")
for role in my_roles:
    if not role:
        continue
    try:
        cur.execute('USE ROLE "{}"'.format(role))
        cur.execute("SHOW DATABASES LIKE 'EDW'")
        dbs = cur.fetchall()
        if dbs:
            print("  Role '{}' CAN see EDW!".format(role))
    except Exception as e:
        pass

print("\n=== Checking what role the Snowflake MCP uses ===")
print("  (The MCP was able to describe the table, so it uses a role with access)")

cur.close()
conn.close()
