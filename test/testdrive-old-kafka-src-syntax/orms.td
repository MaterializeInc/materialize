# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-regex match=s\d+ replacement=<SID>

> CREATE TABLE t (i bigint, t text);

> CREATE DEFAULT INDEX ON t;

> SELECT i.relname as relname, ix.indisunique, ix.indexprs, a.attname, a.attnum, c.conrelid, ix.indkey::varchar, ix.indoption::varchar, i.reloptions, am.amname, pg_get_expr(ix.indpred, ix.indrelid), NULL as indnkeyatts
  FROM pg_class t
  JOIN pg_index ix on t.oid = ix.indrelid
  JOIN pg_class i on i.oid = ix.indexrelid
  LEFT OUTER JOIN pg_attribute a on t.oid = a.attrelid and a.attnum = ANY(ix.indkey)
  LEFT OUTER JOIN pg_constraint c on (ix.indrelid = c.conrelid and ix.indexrelid = c.conindid and c.contype in ('p', 'u', 'x'))
  LEFT OUTER JOIN pg_am am on i.relam = am.oid
  WHERE t.relkind IN ('r', 'v', 'f', 'm', 'p') and t.oid = 't'::regclass and ix.indisprimary = 'f'
  ORDER BY t.relname, i.relname;
t_primary_idx false <null> i 1 <null> "1 2" "0 0" <null> <null> <null> <null>
t_primary_idx false <null> t 2 <null> "1 2" "0 0" <null> <null> <null> <null>

# Check how expressions are serialized with `{}`
> CREATE INDEX complex_index ON t (t::varchar, i::string);

> SELECT ix.indexprs
  FROM pg_class t
  JOIN pg_index ix ON t.oid = ix.indrelid
  WHERE t.oid = 't'::regclass AND ix.indexrelid = 'complex_index'::regclass;
"{t::[<SID> AS pg_catalog.varchar]}, {i::[<SID> AS pg_catalog.text]}"
