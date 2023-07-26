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
nspowner false     oid
nspacl   true      text[]

> SHOW COLUMNS FROM pg_class
name         nullable  type
---------------------------
oid                 false       oid
relname             false       text
relnamespace        false       oid
reloftype           false       oid
relowner            false       oid
relam               false       oid
reltablespace       false       oid
reltoastrelid       false       oid
relhasindex         false       boolean
relpersistence      false       char
relkind             true        text
relchecks           false       smallint
relhasrules         false       boolean
relhastriggers      false       boolean
relhassubclass      false       boolean
relrowsecurity      false       boolean
relforcerowsecurity false       boolean
relreplident        false       char
relispartition      false       boolean
relhasoids          false       boolean
reloptions          true        text[]

> SHOW COLUMNS FROM pg_database
name         nullable  type
---------------------------
 oid            false     oid
 datname        false     text
 datdba         false     oid
 encoding       false     integer
 datistemplate  false     boolean
 datcollate     false     text
 datctype       false     text
 datacl         true      text[]

> SHOW COLUMNS FROM pg_index
name         nullable  type
--------------------------
indexrelid      false       oid
indrelid        false       oid
indisunique     false       boolean
indisprimary    false       boolean
indimmediate    false       boolean
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
attnum       false     smallint
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
contains:function current_schemas() does not exist

> SELECT current_schemas(true)
{mz_catalog,pg_catalog,public}

> SELECT current_schemas(false)
{public}

> SHOW COLUMNS FROM pg_authid
name                nullable    type
------------------------------------------------------------
 oid                false       oid
 rolname            false       text
 rolsuper           true        boolean
 rolinherit         false       boolean
 rolcreaterole      true        boolean
 rolcreatedb        true        boolean
 rolcanlogin        true        boolean
 rolreplication     false       boolean
 rolbypassrls       false       boolean
 rolconnlimit       false       integer
 rolpassword        true        text
 rolvaliduntil      true        "timestamp with time zone"

> SHOW COLUMNS FROM pg_auth_members
name                nullable    type
------------------------------------------------------------
 roleid             false       oid
 member             false       oid
 grantor            false       oid
 admin_option       false       boolean

> SHOW COLUMNS FROM pg_locks
name                nullable    type
------------------------------------------------------------
 locktype           false       text
 database           false       oid
 relation           false       oid
 page               false       integer
 tuple              false       smallint
 virtualxid         false       text
 transactionid      false       text
 classid            false       oid
 objid              false       oid
 objsubid           false       smallint
 virtualtransaction false       text
 pid                false       integer
 mode               false       text
 granted            false       boolean
 fastpath           false       boolean
 waitstart          false       "timestamp with time zone"

> SHOW COLUMNS FROM pg_tablespace
name                nullable    type
------------------------------------------------------------
 oid                false       oid
 spcname            false       text
 spcowner           true        oid
 spcacl             true        text[]
 spcoptions         true        text[]

> SHOW COLUMNS FROM pg_aggregate
name                nullable    type
------------------------------------------------------------
 aggfnoid           false       oid
 aggkind            false       text
 aggnumdirectargs   false       smallint
 aggtransfn         true        regproc
 aggfinalfn         false       regproc
 aggcombinefn       false       regproc
 aggserialfn        false       regproc
 aggdeserialfn      false       regproc
 aggmtransfn        false       regproc
 aggminvtransfn     false       regproc
 aggmfinalfn        false       regproc
 aggfinalextra      false       boolean
 aggmfinalextra     false       boolean
 aggfinalmodify     true        char
 aggmfinalmodify    true        char
 aggsortop          false       oid
 aggtranstype       true        oid
 aggtransspace      true        integer
 aggmtranstype      false       oid
 aggmtransspace     true        integer
 agginitval         true        text
 aggminitval        true        text

> SHOW COLUMNS FROM pg_trigger
name                nullable    type
------------------------------------------------------------
 oid                false       oid
 tgrelid            false       oid
 tgparentid         false       oid
 tgname             false       text
 tgfoid             false       oid
 tgtype             false       smallint
 tgenabled          false       char
 tgisinternal       false       boolean
 tgconstrrelid      false       oid
 tgconstrindid      false       oid
 tgconstraint       false       oid
 tgdeferrable       false       boolean
 tginitdeferred     false       boolean
 tgnargs            false       smallint
 tgattr             false       int2vector
 tgargs             false       bytea
 tgqual             false       text
 tgoldtable         false       text
 tgnewtable         false       text

> SHOW COLUMNS FROM pg_rewrite
name                nullable    type
------------------------------------------------------------
 oid                false       oid
 rulename           false       text
 ev_class           false       oid
 ev_type            false       char
 ev_enabled         false       char
 is_instead         false       boolean
 ev_qual            false       text
 ev_action          false       text

> SHOW COLUMNS FROM pg_extension
name                nullable    type
------------------------------------------------------------
 oid                false       oid
 extname            false       text
 extowner           false       oid
 extnamespace       false       oid
 extrelocatable     false       boolean
 extversion         false       text
 extconfig          false       oid[]
 extcondition       false       text[]

> SHOW COLUMNS FROM pg_event_trigger
name                nullable    type
------------------------------------------------------------
 oid                false       oid
 evtname            false       text
 evtevent           false       text
 evtowner           false       oid
 evtfoid            false       oid
 evtenabled         false       char
 evttags            false       text[]

> SHOW COLUMNS FROM pg_language
name                nullable    type
------------------------------------------------------------
 lanname            false       text
 oid                false       oid
 lanowner           false       oid
 lanispl            false       boolean
 lanpltrusted       false       boolean
 lanplcallfoid      false       oid
 laninline          false       oid
 lanvalidator       false       oid
 lanacl             false       text[]

> SHOW COLUMNS FROM pg_shdescription
name                nullable    type
------------------------------------------------------------
 objoid             false       oid
 classoid           false       oid
 description        false       text

> SHOW COLUMNS FROM pg_depend
name                nullable    type
------------------------------------------------------------
 classid            true        oid
 objid              false       oid
 objsubid           false       integer
 refclassid         true        oid
 refobjid           false       oid
 refobjsubid        false       integer
 deptype            false       char

> SHOW COLUMNS FROM pg_indexes
name                nullable    type
------------------------------------------------------------
 table_catalog      false       text
 schemaname         false       text
 tablename          false       text
 indexname          false       text
 tablespace         true        text
 indexdef           true        text
