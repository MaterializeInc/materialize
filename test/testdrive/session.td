# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SHOW ALL
application_name       ""                                         "Sets the application name to be reported in statistics and logs (PostgreSQL)."
client_encoding        UTF8                                       "Sets the client's character set encoding (PostgreSQL)."
database               materialize                                "Sets the current database (CockroachDB)."
extra_float_digits     3                                          "Adjusts the number of digits displayed for floating-point values (PostgreSQL)."
integer_datetimes      on                                         "Reports whether the server uses 64-bit-integer dates and times (PostgreSQL)."
DateStyle              "ISO, MDY"                                 "Sets the display format for date and time values (PostgreSQL)."
search_path            "mz_catalog, pg_catalog, public, mz_temp"  "Sets the schema search order for names that are not schema-qualified (PostgreSQL)."
server_version         9.5.0                                      "Shows the server version (PostgreSQL)."
sql_safe_updates       false                                      "Prohibits SQL statements that may be overly destructive (CockroachDB)."
TimeZone               UTC                                        "Sets the time zone for displaying and interpreting time stamps (PostgreSQL)."
transaction_isolation  serializable                               "Sets the current transaction's isolation level (PostgreSQL)."

> SET application_name = 'foo'

> SHOW client_encoding
UTF8

> SHOW cLiENt_EnCoDInG
UTF8

! SET client_encoding = UTF9
parameter client_encoding is read only

> SET sql_safe_updates = on
> SHOW sql_safe_updates
true
> SET sql_safe_updates = 'on'
> SET sql_safe_updates = 't'
> SET sql_safe_updates = 'true'
> SET sql_safe_updates = true
> SET sql_safe_updates = 'off'
> SET sql_safe_updates = 'f'
> SET sql_safe_updates = 'false'
> SET sql_safe_updates = false
> SHOW sql_safe_updates
false

# `database` is tested elsewhere.

> SET extra_float_digits = 1

! SET DateStyle = 'ooga booga'
parameter DateStyle is read only

# `search_path` is tested elsewhere.

! SET server_version = "9.6.0"
parameter server_version is read only

! SET TimeZone = 'nope'
parameter TimeZone can only be set to UTC

# The `transaction_isolation` variable has dedicated syntax as mandated by the
# SQL standard.
> SHOW TRANSACTION ISOLATION LEVEL
serializable

! SET transaction_isolation = 'read committed'
parameter transaction_isolation is read only

! SET integer_datetimes = false
parameter integer_datetimes is read only
