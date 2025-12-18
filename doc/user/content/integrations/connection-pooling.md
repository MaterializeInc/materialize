---
title: "Connection Pooling"
description: "How to use connection pooling with Materialize"
menu:
  main:
    parent: "integrations"
    weight: 10
    name: "Connection Pooling"
---

Because Materialize is wire-compatible with PostgreSQL, you can use any
PostgreSQL connection pooler with Materialize. In this guide, we’ll cover how to
use connection pooling with Materialize, alongside the common tool PgBouncer.

## PgBouncer
[PgBouncer](https://www.pgbouncer.org/) is a popular connection pooler for
PostgreSQL. It provides a lightweight and efficient way to manage database
connections, reducing the overhead of establishing new connections and
improving performance.

### Step 1: Install PgBouncer

You can [install PgBouncer](https://www.pgbouncer.org/downloads/) on your local machine or on a server.

### Step 2: Create an authentication userlist file

The userlist file contains the credentials for your Materialize user/app. The file has the format of:

```
"user@example.com" "mypassword-or-scram-secret"
```

#### If using `auth_type = plain` (Cloud and Self-Managed)

Specify the password in plaintext:

- **For Cloud**, use the password from an existing [Service Account](https://materialize.com/docs/security/cloud/users-service-accounts/create-service-accounts/) or generate a [new one](https://materialize.com/docs/security/cloud/users-service-accounts/create-service-accounts/).
- **For Self-Managed**, use the password associated with the role.

Example userlist file:
```
"foo@bar.com" "mypassword"
```

#### If using `auth_type = scram-sha-256` (Self-Managed only)

Specify the SCRAM secret. To find the SCRAM secret, run the following query as a superuser:

```sql
SELECT rolname, rolpassword FROM pg_authid WHERE rolname = 'your_role_name';
```

{{< note >}}
You must be a superuser to access the `pg_authid` table.
{{< /note >}}

Once you have the SCRAM secret, add it to the userlist file in the following format:
```
"your_role_name" "the-hash-you-got-from-pg_authid"
```

### Step 3: Configure PgBouncer to connect to your Materialize instance

1. Locate the configuration file. Refer to the [setup instructions](https://www.pgbouncer.org/downloads/) for your environment.

2. Update the file to:
   - Define a database named `materialize` with your Materialize connection details.
   - Configure PgBouncer as needed for your PgBouncer instance. Be sure to specify the `auth_type` and `auth_file` needed to connect to Materialize.

For example, the following is a basic configuration example to connect a local PgBouncer to a locally-running Materialize:

```ini
[databases]
;; For Cloud, use the connection details from the Console.
;; For Self-Managed, use your Materialize's connection details.
materialize = host=localhost port=6877

[pgbouncer]
logfile = /var/log/pgbouncer/pgbouncer.log
pidfile = /var/run/pgbouncer/pgbouncer.pid
;; Listen on localhost:6432 for incoming connections
listen_addr = localhost
listen_port = 6432

;; Set the authentication type
;; Materialize supports both plain and scram-sha-256.
;; Cloud and Self-Managed support plain.
;; auth_type = plain
;; Self-Managed also supports scram-sha-256:
auth_type = scram-sha-256

;; Set the authentication user list file
auth_file = /etc/pgbouncer/userlist.txt
```

For additional information on configuring PgBouncer, refer to the [PgBouncer documentation](https://www.pgbouncer.org/config.html).

### Step 4: Start the service and connect

After configuring PgBouncer, you can start the service. You can then connect to PgBouncer using the same connection parameters as you would for Materialize, but with the PgBouncer port (default is 6432). For example:

```bash
psql -h localhost -p 6432 -U your_role_name -d materialize
```
