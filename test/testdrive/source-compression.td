# Copyright Materialize, Inc. and contributors. All rights reserved.
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

# Test syntax for no-op compression
> CREATE MATERIALIZED SOURCE static_csv
  FROM FILE '${testdrive.temp-dir}/static.csv'
  COMPRESSION NONE
  FORMAT CSV WITH 3 COLUMNS;

> SELECT * FROM static_csv;
column1        column2  column3  mz_line_no
--------------------------------------------
city           state     zip     1
Rochester      NY        14618   2
"New York"     NY        10004   3
"bad,place\""  CA        92679   4

$ file-append path=static-gzipped.csv.gz compression=gzip
city,state,zip
Rochester,NY,14618
New York,NY,10004
"bad,place""",CA,92679

# Compressed CSV
> CREATE MATERIALIZED SOURCE static_csv_compressed
  FROM FILE '${testdrive.temp-dir}/static-gzipped.csv.gz'
  COMPRESSION GZIP
  FORMAT CSV WITH 3 COLUMNS;

> SELECT * FROM static_csv_compressed;
column1        column2  column3  mz_line_no
--------------------------------------------
city           state     zip     1
Rochester      NY        14618   2
"New York"     NY        10004   3
"bad,place\""  CA        92679   4

# Sources that don't support compression

! CREATE MATERIALIZED SOURCE invalid_compression
  FROM KAFKA BROKER 'foo' TOPIC 'bar'
  COMPRESSION GZIP
  FORMAT AVRO USING SCHEMA 'schema';
contains:Expected end of statement, found COMPRESSION

! CREATE SOURCE f
  FROM KINESIS ARN 'foo'
  COMPRESSION GZIP
  FORMAT BYTES;
contains:Expected end of statement, found COMPRESSION
