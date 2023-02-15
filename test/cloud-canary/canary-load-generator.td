# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> DROP SOURCE IF EXISTS generator1 CASCADE;

> CREATE SOURCE generator1 FROM LOAD GENERATOR COUNTER WITH (SIZE '3xsmall');

> CREATE MATERIALIZED VIEW generator_view1 AS SELECT COUNT(*) AS cnt FROM generator1;

> SELECT cnt > 0 FROM generator_view1;
true

> DROP SOURCE generator1 CASCADE;
