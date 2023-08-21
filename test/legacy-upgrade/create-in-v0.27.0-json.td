# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that any JSON operators in views are properly persisted
#

> CREATE TABLE json_table (a JSONB);

> CREATE MATERIALIZED VIEW json_view AS SELECT a->1 c1, a->>'b' c2, a #> '{b,1}' c3, a #>> '{b, 1}' c4, a - 'b' c5, a @> '{b, 1}' c6, a <@ '{b, 1}'::jsonb c7, a ? 'b' c8 FROM json_table;
