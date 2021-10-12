# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# from create-in-any_version-csv.td
#
# This should be auto-populated with the invalid alias as part of the catalog upgrade
$ file-append path=deleted.csv
definitely,different
1,person

> CREATE MATERIALIZED VIEW breaker AS
  SELECT * FROM csv_upgrade_with_header_alias_missing_file

! SELECT * FROM breaker
source file contains incorrect columns

$ file-append path=changer.csv
no,body
1,person

> CREATE MATERIALIZED VIEW breaker_file_change AS
  SELECT * FROM csv_upgrade_with_file_change

! SELECT * FROM breaker_file_change
source file contains incorrect columns
