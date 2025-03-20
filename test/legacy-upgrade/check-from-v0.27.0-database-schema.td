# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

>[version<11400] SHOW DATABASES;
materialize
"some database"

>[version>=11400] SHOW DATABASES;
materialize ""
"some database" ""

>[version<11400] SHOW SCHEMAS FROM "some database";
public
"some schema"
information_schema
mz_catalog
mz_catalog_unstable
mz_unsafe
mz_internal
mz_introspection
pg_catalog

>[version>=11400] SHOW SCHEMAS FROM "some database";
public ""
"some schema" ""
information_schema ""
mz_catalog ""
mz_catalog_unstable ""
mz_unsafe ""
mz_internal ""
mz_introspection ""
pg_catalog ""

> SELECT * FROM "some database"."some schema".t1;

> DROP DATABASE "some database";

! SELECT * FROM "some database"."some schema".t1;
contains:unknown database
