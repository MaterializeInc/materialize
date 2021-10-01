# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ file-append path=csv.csv
id,value
1,person

> CREATE MATERIALIZED SOURCE csv_upgrade_with_header
  FROM FILE '${testdrive.temp-dir}/csv.csv'
  FORMAT CSV WITH HEADER

> CREATE MATERIALIZED SOURCE csv_upgrade_with_header_alias (al, sia)
  FROM FILE '${testdrive.temp-dir}/csv.csv'
  FORMAT CSV WITH HEADER

> CREATE MATERIALIZED SOURCE csv_upgrade_no_header
  FROM FILE '${testdrive.temp-dir}/csv.csv'
  FORMAT CSV WITH 2 COLUMNS

> CREATE MATERIALIZED SOURCE csv_upgrade_no_header_alias (al, sia)
  FROM FILE '${testdrive.temp-dir}/csv.csv'
  FORMAT CSV WITH 2 COLUMNS
