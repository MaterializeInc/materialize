# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ file-append path=deleted.csv
id,present
1,person

> CREATE SOURCE csv_upgrade_with_header_alias_missing_file
  FROM FILE '${testdrive.temp-dir}/deleted.csv'
  FORMAT CSV WITH HEADER

$ file-delete path=deleted.csv
