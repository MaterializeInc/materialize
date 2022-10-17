# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> select * from csv_upgrade_no_header
column1  column2
------------------
id       value
1        person

> select * from csv_upgrade_no_header_alias
al  sia
------------
id  value
1   person
