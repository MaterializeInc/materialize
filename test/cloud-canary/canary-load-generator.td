# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> DROP SOURCE IF EXISTS generator1 CASCADE;

> CREATE CLUSTER canary_load_generator SIZE '3xsmall';

> CREATE SOURCE generator1 IN CLUSTER canary_load_generator FROM LOAD GENERATOR COUNTER;

> CREATE MATERIALIZED VIEW generator_view1 AS SELECT COUNT(*) AS cnt FROM generator1;

> CREATE DEFAULT INDEX ON generator_view1;

> SELECT cnt > 0 FROM generator_view1;
true
