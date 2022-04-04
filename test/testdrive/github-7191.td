# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The presence of this query in a view tests that the transformation of
# "pow" -> "power" is correctly planned on reboot when testdrive is run with
# --validate-data-dir. This protects against regression of #7191.
> CREATE VIEW v AS SELECT pow(2, 4)
> SELECT * FROM v
16
