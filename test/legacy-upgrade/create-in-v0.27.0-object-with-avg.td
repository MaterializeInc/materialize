# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ skip-if
SELECT mz_version_num() >= 6700;

> CREATE VIEW view_with_avg_internal (a) AS SELECT AVG(position) FROM mz_catalog.mz_columns;

> CREATE MATERIALIZED VIEW mat_view_with_avg_internal (a) AS SELECT AVG(position) FROM mz_catalog.mz_columns;
