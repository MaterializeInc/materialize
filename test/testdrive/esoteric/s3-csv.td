! CREATE SOURCE mismatched_column_count
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-static-${testdrive.seed}'
  FORMAT CSV WITH 2 COLUMNS
contains:Specified column count (WITH 2 COLUMNS) does not match number of columns in CSV file (3)

! CREATE SOURCE mismatched_column_names
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-static-${testdrive.seed}'
  FORMAT CSV WITH HEADER (cities, country, zip)
contains:Header columns do not match named columns from CREATE SOURCE statement. First mismatched columns: cities != city

! CREATE SOURCE mismatched_column_names_count
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-static-${testdrive.seed}'
  FORMAT CSV WITH HEADER (cities, state)
contains:Named column count (2) does not match number of columns discovered (3)

# Static CSV with automatic headers.
> CREATE MATERIALIZED SOURCE static_csv_header
  FROM FILE '${testdrive.temp-dir}/static.csv'
  FORMAT CSV WITH HEADERS

> SELECT * FROM static_csv_header
city           state     zip     mz_line_no
--------------------------------------------
Rochester      NY        14618   1
"New York"     NY        10004   2
"bad,\nplace\""  CA        92679   3

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
Rochester      NY        14618   1

$ file-append path=dynamic.csv
New York,NY,10004

> SELECT * FROM dynamic_csv
city           state     zip     mz_line_no
--------------------------------------------
Rochester      NY        14618   1
"New York"     NY        10004   2

$ file-append path=deleting.csv
city,state,zip
Tucson,AZ,85719

> CREATE SOURCE deleting_csv
  FROM FILE '${testdrive.temp-dir}/deleting.csv'
  FORMAT CSV WITH HEADER

$ file-delete path=deleting.csv

> CREATE INDEX i ON deleting_csv(city,state,zip)

! SELECT * FROM deleting_csv
regex:Source error: .*: file IO: file source: unable to open file at path

! CREATE SOURCE should_fail FROM FILE '/'
contains:/ is a directory.

# Static CSV with manual headers.
> CREATE MATERIALIZED SOURCE static_csv_manual_header (city_man, state_man, zip_man)
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-static-${testdrive.seed}'
  FORMAT CSV WITH HEADER

> SELECT * FROM static_csv_manual_header
city_man       state_man  zip_man  mz_line_no
---------------------------------------------
Rochester      NY         14618    1
"New York"     NY         10004    2
"bad,\nplace\""  CA         92679    3

