# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ file-append path=static.csv
city,state,zip
Rochester,NY,14618
New York,NY,10004
"bad,place""",CA,92679

# Static CSV without headers.
> CREATE MATERIALIZED SOURCE static_csv
  FROM FILE '${testdrive.temp-dir}/static.csv'
  FORMAT CSV WITH 3 COLUMNS

> SELECT * FROM static_csv
column1        column1  column2  mz_line_no
--------------------------------------------
city           state     zip     1
Rochester      NY        14618   2
"New York"     NY        10004   3
"bad,place\""  CA        92679   4

# Static CSV with automatic headers.
> CREATE MATERIALIZED SOURCE static_csv_header
  FROM FILE '${testdrive.temp-dir}/static.csv'
  FORMAT CSV WITH HEADER

> SELECT * FROM static_csv_header
city           state     zip     mz_line_no
--------------------------------------------
Rochester      NY        14618   2
"New York"     NY        10004   3
"bad,place\""  CA        92679   4

# Static CSV with manual headers.
> CREATE MATERIALIZED SOURCE static_csv_manual_header (city_man, state_man, zip_man)
  FROM FILE '${testdrive.temp-dir}/static.csv'
  FORMAT CSV WITH HEADER

> SELECT * FROM static_csv_manual_header
city_man       state_man  zip_man  mz_line_no
---------------------------------------------
Rochester      NY         14618    2
"New York"     NY         10004    3
"bad,place\""  CA         92679    4

# Dynamic CSV with automatic headers.

$ file-append path=dynamic.csv
city,state,zip

> CREATE MATERIALIZED SOURCE dynamic_csv
  FROM FILE '${testdrive.temp-dir}/dynamic.csv' WITH (tail = true)
  FORMAT CSV WITH HEADER

> SELECT * FROM dynamic_csv

$ file-append path=dynamic.csv
Rochester,NY,14618

> SELECT * FROM dynamic_csv
city           state     zip     mz_line_no
--------------------------------------------
Rochester      NY        14618   2

$ file-append path=dynamic.csv
New York,NY,10004

> SELECT * FROM dynamic_csv
city           state     zip     mz_line_no
--------------------------------------------
Rochester      NY        14618   2
"New York"     NY        10004   3
