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
oid                 false       oid
relname             false       text
relnamespace        false       oid
reloftype           false       oid
relowner            true        oid
relam               false       oid
reltablespace       false       oid
reltoastrelid       false       oid
relhasindex         false       boolean
relpersistence      false       char
relkind             true        text
relchecks           false       smallint
relhasrules         false       boolean
relhastriggers      false       boolean
relrowsecurity      false       boolean
relforcerowsecurity false       boolean
relreplident        false       char
relispartition      false       boolean
relhasoids          false       boolean
reloptions          true        text[]

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
indexrelid      false       oid
indrelid        false       oid
indisprimary    false       boolean
indisunique     false       boolean
indisclustered  false       boolean
indisvalid      false       boolean
indisreplident  false       boolean
indkey          false       int2vector
indoption       false       int2vector
indexprs        true        text
indpred         true        text

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
atttypid     false     oid
attlen       true      smallint
attnum       false     bigint
atttypmod    false     integer
attnotnull   false     boolean
atthasdef    false     boolean
attidentity  false     char
attgenerated false     char
attisdropped false     boolean
attcollation false     oid

> SHOW COLUMNS FROM pg_collation
name         nullable  type
---------------------------
oid                 false   oid
collname            false   text
collnamespace       false   oid
collowner           false   oid
collprovider        false   char
collisdeterministic false   boolean
collencoding        false   integer
collcollate         false   text
collctype           false   text
collversion         false   text

> SHOW COLUMNS FROM pg_policy
name         nullable  type
---------------------------
oid             false   oid
polname         false   text
polrelid        false   oid
polcmd          false   char
polpermissive   false   boolean
polroles        false   oid[]
polqual         false   text
polwithcheck    false   text

> SHOW COLUMNS FROM pg_inherits
name         nullable  type
---------------------------
inhrelid            false   oid
inhparent           false   oid
inhseqno            false   integer
inhdetachpending    false   boolean

! SELECT current_schemas()
contains:Cannot call function current_schemas(): arguments cannot be implicitly cast to any implementation's parameters;

> SELECT current_schemas(true)
{mz_catalog,pg_catalog,public,mz_temp}

> SELECT current_schemas(false)
{public}
