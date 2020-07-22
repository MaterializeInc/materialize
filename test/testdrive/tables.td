# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE TABLE t (a int, b text NOT NULL)

> SELECT * FROM t;

> SHOW TABLES;
TABLES
------
t

> SHOW SOURCES;
SOURCES
-------
t

> SHOW INDEXES FROM t;
Source_or_view        Key_name                          Column_name  Expression  Null   Seq_in_index
----------------------------------------------------------------------------------------------------------------
materialize.public.t  materialize.public.t_primary_idx  a            <null>      true              1
materialize.public.t  materialize.public.t_primary_idx  b            <null>      false             2

> SHOW COLUMNS in t;
Field      Nullable  Type
-------------------------
a          YES       int4
b          NO        text

> DROP TABLE t;

! SELECT * from t;
unknown catalog item 't'

! SHOW INDEXES FROM t;
unknown catalog item 't'

> SHOW TABLES;

> SHOW SOURCES;
