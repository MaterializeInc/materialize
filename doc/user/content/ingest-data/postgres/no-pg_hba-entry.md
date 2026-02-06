---
title: "Troubleshooting: No pg_hba.conf entry"
description: "How to troubleshoot and resolve the no pg_hba.conf entry error with PostgreSQL sources in Materialize"
menu:
  main:
    parent: "pg-troubleshooting"
    name: "No pg_hba.conf entry"
    weight: 20
---

This guide helps you troubleshoot and resolve the "FATAL: no pg_hba.conf entry"
error that can occur when connecting PostgreSQL sources to Materialize.

## What this error means

When you see an error like:

```nofmt
postgres: FATAL: no pg_hba.conf entry for host "10.0.1.45", user "materialize", database "production", SSL off
```

This means that PostgreSQL's host-based authentication (HBA) configuration is
rejecting Materialize's connection attempt. The `pg_hba.conf` file controls
which hosts are allowed to connect to PostgreSQL, which databases they can
access, which users they can authenticate as, and what authentication methods
are required. When Materialize tries to establish a replication connection and
no matching rule exists in `pg_hba.conf`, PostgreSQL immediately rejects the
connection with this error.

## Common causes

- **Missing or incomplete pg_hba.conf entry**: The most common cause is that
  there is no entry in `pg_hba.conf` that allows connections from Materialize's
  IP address or network range for the specific database and user combination.
- **Incorrect IP address or network range**: The `pg_hba.conf` entry may exist
  but specify the wrong IP address, CIDR range, or use a hostname that doesn't
  resolve to Materialize's actual connection address.
- **SSL/TLS mismatch**: Materialize attempts to connect with SSL but the
  `pg_hba.conf` entry requires `hostnossl`, or vice versa. The error message
  will indicate "SSL on" or "SSL off" to help identify this.
- **Incorrect database or user specification**: The `pg_hba.conf` entry may
  allow the user or database individually, but not the specific combination
  that Materialize is using.
- **Entry order matters**: PostgreSQL processes `pg_hba.conf` entries from top
  to bottom and uses the first matching entry. A more restrictive rule earlier
  in the file may be matching before your intended permissive rule.
- **Dynamic or ephemeral IP addresses**: If Materialize's connection originates
  from dynamic IP addresses (common in cloud environments), the `pg_hba.conf`
  entry may become stale as IP addresses change.
- **Firewall or network changes**: Changes to network infrastructure may cause
  Materialize to connect from a different IP address than originally configured.
- **Replication-specific requirements**: For logical replication, you need a
  `host` or `hostssl` entry for `replication`, not just for the database.

## Diagnosing the issue

### Review the error message details

The error message contains crucial information:

- **host**: The IP address Materialize is connecting from
- **user**: The PostgreSQL user Materialize is authenticating as
- **database**: The database Materialize is trying to access (may show
  "replication" for replication connections)
- **SSL on/off**: Whether the connection attempt is using SSL/TLS

### Check your current pg_hba.conf configuration

Connect to your PostgreSQL server and examine the current `pg_hba.conf` file:

```sql
-- View the current pg_hba.conf settings
SELECT * FROM pg_hba_file_rules;
```

Look for:

- Entries that match your database and user
- The IP address ranges allowed for each entry
- Whether SSL is required (`hostssl`) or prohibited (`hostnossl`)
- The order of entries (earlier entries take precedence)

Alternatively, locate and view the file directly:

```sql
-- Find the location of pg_hba.conf
SHOW hba_file;
```

### Verify Materialize's connection IP address

Check what IP address PostgreSQL sees from Materialize:

```sql
SELECT
  client_addr,
  usename,
  application_name,
  state
FROM pg_stat_activity
WHERE usename = 'materialize';
```

This shows you the actual IP address that successful connections use, which
should match what `pg_hba.conf` allows.

## Resolution

### Add or update pg_hba.conf entry

Edit your PostgreSQL `pg_hba.conf` file to add an entry that allows
Materialize's connection. The entry should match the connection parameters from
your error message.

**For SSL connections (recommended):**

```nofmt
# Allow Materialize replication connection with SSL
hostssl    all    materialize    10.0.1.0/24    md5
hostssl    replication    materialize    10.0.1.0/24    md5
```

**For non-SSL connections:**

```nofmt
# Allow Materialize replication connection without SSL
host    all    materialize    10.0.1.0/24    md5
host    replication    materialize    10.0.1.0/24    md5
```

**Important configuration notes:**

- Replace `10.0.1.0/24` with the actual IP address or CIDR range from your
  error message.
- Use `hostssl` instead of `host` if you want to require SSL/TLS encryption.
- Include both database access and replication entries for full functionality.
- Replace `md5` with your preferred authentication method (`scram-sha-256` is
  more secure if supported).
- For broader access, you can use `0.0.0.0/0` to allow any IP, but this is
  **not recommended** for production.

### Reload PostgreSQL configuration

After editing `pg_hba.conf`, reload the configuration without restarting
PostgreSQL:

```sql
-- Reload the configuration
SELECT pg_reload_conf();
```

Alternatively, from the command line:

```bash
pg_ctl reload
```

### Verify the change

After reloading, check that your new entry is active:

```sql
-- Verify the updated rules
SELECT
  line_number,
  type,
  database,
  user_name,
  address,
  auth_method
FROM pg_hba_file_rules
WHERE user_name = '{materialize}' OR user_name = '{all}';
```

Materialize should automatically retry the connection and succeed once the
configuration is updated.

## Prevention

**Best practices to avoid this error:**

- **Use CIDR ranges for cloud deployments**: Instead of specific IP addresses,
  use CIDR ranges that accommodate IP address changes in cloud environments.
  Materialize provides a list of IP addresses used for connections that you can
  configure in advance.
- **Require SSL/TLS connections**: Use `hostssl` entries to ensure encrypted
  connections and configure Materialize sources to use SSL to match.
- **Place Materialize entries appropriately**: Position Materialize's
  `pg_hba.conf` entries after any restrictive rules but before overly
  permissive catch-all rules.
- **Configure replication access**: Ensure you have entries for both the
  database and for `replication` to support logical replication:

```sql
-- Monitor active replication connections
SELECT
  client_addr,
  usename,
  state,
  sync_state
FROM pg_stat_replication;
```

- **Test after network changes**: After any network infrastructure changes,
  verify that Materialize can still connect before the next replication window.
- **Use connection pooling carefully**: If using a connection pooler between
  Materialize and PostgreSQL, ensure `pg_hba.conf` allows connections from the
  pooler's IP address, not Materialize's.
- **Review pg_hba.conf regularly**: Periodically audit your `pg_hba.conf` file
  to remove stale entries and ensure current entries are still accurate.
