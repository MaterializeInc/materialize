# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SHOW ALL
application_name            ""                                         "Sets the application name to be reported in statistics and logs (PostgreSQL)."
client_encoding             UTF8                                       "Sets the client's character set encoding (PostgreSQL)."
database                    materialize                                "Sets the current database (CockroachDB)."
extra_float_digits          3                                          "Adjusts the number of digits displayed for floating-point values (PostgreSQL)."
failpoints                  ""                                         "Allows failpoints to be dynamically activated."
integer_datetimes           on                                         "Reports whether the server uses 64-bit-integer dates and times (PostgreSQL)."
DateStyle                   "ISO, MDY"                                 "Sets the display format for date and time values (PostgreSQL)."
search_path                 "mz_catalog, pg_catalog, public, mz_temp"  "Sets the schema search order for names that are not schema-qualified (PostgreSQL)."
server_version              9.5.0                                      "Shows the server version (PostgreSQL)."
server_version_num          90500                                      "Shows the server version as an integer (PostgreSQL)."
sql_safe_updates            off                                        "Prohibits SQL statements that may be overly destructive (CockroachDB)."
standard_conforming_strings on                                         "Causes '...' strings to treat backslashes literally (PostgreSQL)."
TimeZone                    UTC                                        "Sets the time zone for displaying and interpreting time stamps (PostgreSQL)."
transaction_isolation       serializable                               "Sets the current transaction's isolation level (PostgreSQL)."

> SET application_name = 'foo'

> SHOW client_encoding
UTF8

> SHOW cLiENt_EnCoDInG
UTF8

! SET client_encoding = UTF9
contains:parameter "client_encoding" can only be set to "UTF8"

# if its utf8 we let it through
> SET NAMES 'UTF8';

# match the behavior of postgres as specified here: https://www.postgresql.org/docs/9.1/sql-set.html
! SET NAMES = "something";
contains:unrecognized configuration parameter "names"

> SET sql_safe_updates = on
> SHOW sql_safe_updates
on
> SET sql_safe_updates = 'on'
> SET sql_safe_updates = 't'
> SET sql_safe_updates = 'true'
> SET sql_safe_updates = true
> SET sql_safe_updates = 'off'
> SET sql_safe_updates = 'f'
> SET sql_safe_updates = 'false'
> SET sql_safe_updates = false
> SHOW sql_safe_updates
off

# `database` is tested elsewhere.

> SET extra_float_digits = 1

> SET DateStyle = 'ISO'
> SET DateStyle = 'MDY'
> SET DateStyle = 'ISO,MDY'
! SET DateStyle = 'ooga booga'
contains:parameter "DateStyle" can only be set to "ISO, MDY"

# `search_path` is tested elsewhere.

! SET server_version = "9.6.0"
contains:parameter "server_version" cannot be changed

! SET server_version_num = "90600"
contains:parameter "server_version_num" cannot be changed

! SET TimeZone = 'nope'
contains:parameter "TimeZone" can only be set to "UTC"

# The `transaction_isolation` variable has dedicated syntax as mandated by the
# SQL standard.
> SHOW TRANSACTION ISOLATION LEVEL
serializable

! SET transaction_isolation = 'read committed'
contains:parameter "transaction_isolation" cannot be changed

! SET integer_datetimes = false
contains:parameter "integer_datetimes" cannot be changed
