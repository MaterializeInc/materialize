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

> CREATE INDEX complex_index ON t (t, i::string);

# check the indkey answers both as an int2vec and cast to a string
> SELECT ix.indkey, ix.indkey::text
  FROM pg_class t
  JOIN pg_index ix ON t.oid = ix.indrelid
  WHERE t.oid = 't'::regclass AND ix.indexrelid = 'complex_index'::regclass;
"[0:1]={2,0}" "2 0"
