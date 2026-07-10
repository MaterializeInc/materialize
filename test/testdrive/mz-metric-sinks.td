# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify the `mz_metric_sinks` builtin table and `SHOW METRIC SINKS`.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_metric_sink = true;

> CREATE VIEW v AS SELECT
    'm'::text AS metric_name,
    'gauge'::text AS metric_type,
    '{}'::map[text=>text] AS labels,
    1.0::double AS value,
    'h'::text AS help

> CREATE METRIC SINK s IN CLUSTER quickstart FROM v

> SELECT name FROM mz_catalog.mz_metric_sinks
s

> SHOW METRIC SINKS
s quickstart ""

> DROP METRIC SINK s

> SELECT count(*) FROM mz_catalog.mz_metric_sinks
0

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET enable_metric_sink;
