# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Check that all data sources empty out as the underlying Postgres tables
# become empty.
#

> SELECT COUNT(*) = 0 FROM pk_table;
true

> SELECT COUNT(*) = 0 FROM nonpk_table;
true

> SELECT COUNT(*) = 0 FROM large_text;
true

> SELECT COUNT(*) = 0 FROM trailing_space_nopk;
true

> SELECT COUNT(*) = 0 FROM trailing_space_pk;
true

> SELECT COUNT(*) = 0 FROM multipart_pk;
true

> SELECT COUNT(*) = 0 FROM nulls_table;
true

> SELECT COUNT(*) = 0 FROM utf8_table;
true

> SELECT COUNT(*) = 0 FROM join_view;
true
