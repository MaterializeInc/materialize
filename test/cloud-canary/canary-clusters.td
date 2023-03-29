# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> DROP CLUSTER IF EXISTS cluster1 CASCADE;

> CREATE CLUSTER cluster1 REPLICAS (replica1 (SIZE '3xsmall'), replica2 (SIZE '3xsmall'));

> SET cluster=cluster1;

> SHOW CLUSTERS;
mz_system
mz_introspection
default
cluster1

> SHOW CLUSTER REPLICAS WHERE cluster = 'cluster1';
cluster1 replica1 3xsmall true
cluster1 replica2 3xsmall true

> CREATE MATERIALIZED VIEW cluster_view1 AS SELECT COUNT(*) AS cnt FROM mz_tables;

> CREATE DEFAULT INDEX ON cluster_view1;

> SELECT cnt > 0 FROM cluster_view1;
true

> DROP CLUSTER cluster1 CASCADE;
