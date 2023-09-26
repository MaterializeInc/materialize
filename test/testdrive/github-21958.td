# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that table creation involving types that depend on other types correctly describe their
# resolved_ids on restart. This is asserted by testdrive during its catalog verification step after
#each DDL.

> CREATE TYPE point AS (x integer, y integer);
> CREATE TYPE custom_map AS MAP (KEY TYPE = text, VALUE TYPE = bool);
> CREATE TABLE t (c1 point, c2 custom_map, c3 integer[], c4 map[text=>map[text=>map[text=>text]]]);
