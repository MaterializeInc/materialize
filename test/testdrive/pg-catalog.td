# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SHOW COLUMNS FROM pg_namespace
name     nullable  type
--------------------------
oid      false     oid
nspname  false     text
nspowner true      oid
nspacl   true      text[]

> SHOW COLUMNS FROM pg_class
name         nullable  type
---------------------------
oid          false     oid
relname      false     text
relnamespace false     oid
relowner     true      oid
relam        false     oid
relkind      true      text

> SHOW COLUMNS FROM pg_database
name         nullable  type
---------------------------
 oid         false     oid
 datname     false     text
 datdba      true      oid
 encoding    false     integer
 datcollate  false     text
 datctype    false     text
 datacl      true      text[]

> SHOW COLUMNS FROM pg_index
name         nullable  type
--------------------------
indexrelid   false     oid
indrelid     false     oid
indisprimary false     boolean
indkey       false     bigint[]

> SHOW COLUMNS FROM pg_description
name         nullable  type
---------------------------
objoid       false     oid
classoid     true      oid
objsubid     false     integer
description  true      text

> SHOW COLUMNS FROM pg_attribute
name         nullable  type
---------------------------
attrelid     false     oid
attname      false     text
attnum       false     bigint
atttypmod    false     integer
attnotnull   false     boolean
attisdropped false     boolean
atttypid     false     oid
attlen       true      smallint
atthasdef    false     boolean
attidentity  false     character

! SELECT current_schemas()
contains:Cannot call function current_schemas(): arguments cannot be implicitly cast to any implementation's parameters;

> SELECT current_schemas(true)
{mz_catalog,pg_catalog,public,mz_temp}

> SELECT current_schemas(false)
{public}
