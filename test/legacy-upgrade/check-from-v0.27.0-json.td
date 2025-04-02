# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SHOW CREATE MATERIALIZED VIEW json_view;
materialize.public.json_view "CREATE MATERIALIZED VIEW materialize.public.json_view\n    IN CLUSTER json_compute_cluster\n    WITH (REFRESH = ON COMMIT)\n    AS\n        SELECT\n            a -> 1 AS c1,\n            a ->> 'b' AS c2,\n            a #> '{b,1}' AS c3,\n            a #>> '{b, 1}' AS c4,\n            a - 'b' AS c5,\n            a @> '{b, 1}' AS c6,\n            a <@ '{b, 1}'::pg_catalog.jsonb AS c7,\n            a ? 'b' AS c8\n        FROM materialize.public.json_table;"
