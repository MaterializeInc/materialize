# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# CREATE a view containing subqueries/derived tables of various types

> SHOW CREATE MATERIALIZED VIEW subquery_view;
"materialize.public.subquery_view" "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"subquery_view\" IN CLUSTER \"default\" AS SELECT (SELECT 1) FROM (SELECT 2) AS \"derived\" WHERE 2 NOT IN (SELECT 3) AND NOT EXISTS (SELECT 4) AND 5 >= ALL (SELECT 6) AND 7 <ANY (SELECT 8)"
