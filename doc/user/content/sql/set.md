---
title: "SET"
description: "`SET` a session variable value in Materialize."
menu:
  main:
    parent: 'commands'

---

The `SET` command modifies a session variable value. Session variables store information about the user, application state, or preferences like target cluster, search path, or transaction isolation during the session lifetime.

## Syntax
{{< diagram "set-session-variable.svg" >}}

Field | Use
------|-----
_variable&lowbar;name_ | The session variable name to modify. For the available session variable names, see [the following table](#variables).
_variable&lowbar;value_ | The value to assign to the session variable.


## Variables

Name                                        | Default Value             | Description   |
--------------------------------------------|---------------------------|---------------|
application_name                            | Empty string              | The application name to be reported in statistics and logs.
client_encoding                             | `UTF8`                      | The client's character set encoding.
client_min_messages                         | `Notice`                  | The message levels that are sent to the client. <br/><br/> Accepted values: `Error`, `Warning`, `Notice`, `Log`, `Debug1`, `Debug2`, `Debug3`, `Debug4`, `Debug5`
cluster                                     | `default`                 | The current cluster.
cluster_replica                             | Empty string              | The target cluster replica for SELECT queries.
database                                    | `materialize`             | The current database.
datestyle                                   | `ISO, MDY`                | The display format for date and time values
emit_timestamp_notice                       | `false`                   | Boolean flag indicating whether to send a `NOTICE` specifying query timestamps.
emit_trace_id_notice                        | `false`                      | Boolean flag indicating whether to send a `NOTICE` specifying the trace id when available
extra_float_digits                          | `3`                       | Adjusts the number of digits displayed for floating-point values.
failpoints                                  | Empty string              | Allows failpoints to be dynamically activated.
integer_datetimes                           | `true`                    | Reports whether the server uses 64-bit-integer dates and times.
intervalstyle                               | `postgres`                | The display format for interval values.
search_path                                 | `public`                  | The schema search order for names that are not schema-qualified.
server_version                              | Version-dependent         | The server version.
server_version_num                          | Version-dependent         | The server version as an integer.
sql_safe_updates                            | `false`                   | Prohibits SQL statements that may be overly destructive.
standard_conforming_strings                 | `true`                    | Causes `'...'` strings to treat backslashes literally.
statement_timeout                           | `10 seconds`              | The maximum allowed duration of `INSERT`, `SELECT`, `UPDATE`, and `DELETE` operations. This session variable **has no effect** and its sole purpose is to maintain compatibility with Postgres.
idle_in_transaction_session_timeout         | `120 seconds`             | The maximum allowed duration that a session can sit idle in a transaction before being terminated. A value of zero disables the timeout. This session variable **has no effect** and its sole purpose is to maintain compatibility with Postgres.
transaction_isolation                       | `STRICT SERIALIZABLE`     | The current [transaction's isolation level.](/overview/isolation-level/)
timezone                                    | `UTC`                     | The time zone for displaying and interpreting time stamps. This session variable **has no effect** and its sole purpose is to maintain compatibility with Postgres.

### Examples

#### `SET` Cluster

```sql
SET CLUSTER = 'default';
```

#### `SET` transaction isolation

```sql
SET TRANSACTION_ISOLATION TO 'STRICT SERIALIZABLE';
```
