# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

> SHOW ALL
application_name  ""                  "Sets the application name to be reported in statistics and logs (PostgreSQL)."
client_encoding   UTF8                "Sets the client's character set encoding (PostgreSQL)."
database          default             "Sets the current database (CockroachDB)."
DateStyle         "ISO, MDY"          "Sets the display format for date and time values (PostgreSQL)."
server_version    9.5.0               "Shows the server version (PostgreSQL)."
sql_safe_updates  false               "Prohibits SQL statements that may be overly destructive (CockroachDB)."

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
