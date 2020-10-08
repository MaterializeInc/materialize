# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SHOW COLUMNS FROM pg_namespace
Field    Nullable  Type
--------------------------
oid      NO        oid
nspname  NO        text
nspowner YES       oid
nspacl   YES       _text

> SHOW COLUMNS FROM pg_class
Field        Nullable  Type
--------------------------
oid          NO        oid
relname      NO        text
relnamespace NO        oid
relowner     YES       oid
relkind      YES       text

> SHOW COLUMNS FROM pg_database
Field        Nullable  Type
--------------------------
 oid         NO        oid
 datname     NO        text
 datdba      YES       oid
 encoding    NO        int4
 datcollate  NO        text
 datctype    NO        text
 datacl      YES       _text

! SELECT current_schemas()
Cannot call function current_schemas(): arguments cannot be implicitly cast to any implementation's parameters;

> SELECT current_schemas(true)
{mz_catalog,pg_catalog,public,mz_temp}

> SELECT current_schemas(false)
{public}
