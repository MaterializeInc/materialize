# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for https://github.com/MaterializeInc/materialize/issues/6950

> CREATE SCHEMA some_schema

! CREATE TEMPORARY VIEW IF NOT EXISTS some_schema.v1 AS SELECT 1;
contains:cannot create temporary item in non-temporary schema
