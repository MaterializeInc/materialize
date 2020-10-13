# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SHOW COLUMNS FROM pg_namespace
field    nullable  type
--------------------------
oid      NO        oid
nspname  NO        text
nspowner YES       oid
nspacl   YES       _text

> SHOW COLUMNS FROM pg_class
field        nullable  type
--------------------------
oid          NO        oid
relname      NO        text
relnamespace NO        oid
relowner     YES       oid
relkind      YES       text

> SHOW COLUMNS FROM pg_database
field        nullable  type
--------------------------
 oid         NO        oid
 datname     NO        text
 datdba      YES       oid
 encoding    NO        int4
 datcollate  NO        text
 datctype    NO        text
 datacl      YES       _text

> SHOW COLUMNS FROM pg_index
field        nullable  type
--------------------------
indexrelid   NO        oid
indrelid     NO        oid

> SHOW COLUMNS FROM pg_description
field        nullable  type
--------------------------
objoid       NO        oid
classoid     YES       oid
objsubid     NO        int4
description  YES       text

> SHOW COLUMNS FROM pg_attribute
field        nullable  type
--------------------------
attrelid     NO        oid
attname      NO        text
attnum       NO        int8
attnotnull   NO        bool

! SELECT current_schemas()
Cannot call function current_schemas(): arguments cannot be implicitly cast to any implementation's parameters;

> SELECT current_schemas(true)
{mz_catalog,pg_catalog,public,mz_temp}

> SELECT current_schemas(false)
{public}
