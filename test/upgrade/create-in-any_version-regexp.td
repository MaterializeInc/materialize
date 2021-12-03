# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that any regexp operators in views are properly persisted
#

> CREATE TABLE regexp_table (a STRING);

> CREATE MATERIALIZED VIEW regexp_view AS SELECT a NOT LIKE 'b' c1, a ILIKE 'b' c2, a ~ 'b' c3, a ~* 'b' c4, a !~ 'b' c5, a !~* 'b' c6 FROM regexp_table;
