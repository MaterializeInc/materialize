# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test pg_function_is_visible. Currently relies on _pg_expandarray living in the information_schema schema which is true
# in postgres as well.

> SELECT DISTINCT name, pg_table_is_visible(oid) AS visible FROM mz_catalog.mz_functions where name = '_pg_expandarray'
name               visible
--------------------------
_pg_expandarray    false
> SET search_path=information_schema,public
> SELECT DISTINCT name, pg_table_is_visible(oid) AS visible FROM mz_catalog.mz_functions where name = '_pg_expandarray'
name               visible
--------------------------
_pg_expandarray    true
