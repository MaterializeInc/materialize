# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1minute

$ file-append path=data-consistency.csv
dummy,1,0,0,0

$ file-append path=data.csv
a,b
1,1
2,2
3,3
4,4

$ file-append path=data2.csv
a,b
1,1
2,2

# Static CSV
> CREATE MATERIALIZED SOURCE data_byo FROM FILE '${testdrive.temp-dir}/data.csv'
    WITH (consistency = '${testdrive.temp-dir}/data-consistency.csv', tail=true)
    FORMAT CSV WITH HEADER

> CREATE MATERIALIZED VIEW view_byo AS SELECT * FROM data_byo

> CREATE MATERIALIZED SOURCE data_byo2 (a,b) FROM FILE '${testdrive.temp-dir}/data2.csv'
    WITH (consistency = '${testdrive.temp-dir}/data-consistency.csv', tail=true)
    FORMAT CSV WITH HEADER

> CREATE MATERIALIZED VIEW view_byo2 AS SELECT * FROM data_byo2

! SELECT * FROM view_byo;
At least one input has no complete timestamps yet:

$ file-append path=data-consistency.csv
\${testdrive.temp-dir}/data.csv,1,0,1,2
\${testdrive.temp-dir}/data2.csv,1,0,1,2
\${testdrive.temp-dir}/data2.csv,1,0,2,5

> CREATE MATERIALIZED SOURCE data_byo3 FROM FILE '${testdrive.temp-dir}/data2.csv'
    WITH (consistency = '${testdrive.temp-dir}/data-consistency.csv')
    FORMAT CSV WITH HEADER

> SELECT * FROM view_byo
a   b   mz_line_no
--------------
1   1   2

> SELECT * FROM view_byo2
a   b   mz_line_no
--------------
1   1  2

> CREATE MATERIALIZED VIEW view_byo3 AS SELECT * FROM data_byo3

$ file-append path=data2.csv
3,3
4,4

> SELECT * FROM view_byo2
a   b   mz_line_no
--------------
1   1  2
2   2  3
3   3  4
4   4  5

> SELECT * FROM view_byo3
a   b   mz_line_no
--------------
1   1  2

$ file-append path=data-consistency.csv
\${testdrive.temp-dir}/data.csv,1,0,2,5


> SELECT * FROM view_byo
a   b   mz_line_no
--------------
1   1   2
2   2   3
3   3   4
4   4   5
