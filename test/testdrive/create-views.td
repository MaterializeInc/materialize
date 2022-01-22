# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# General CREATE VIEW testing
#

> CREATE TABLE t (a INT, b INT);
> INSERT INTO t VALUES (1, 2);

! CREATE VIEW t_v AS SELECT a, a FROM t;
contains:column "a" specified more than once

! CREATE VIEW t_v AS SELECT b, a, b FROM t;
contains:column "b" specified more than once

! CREATE VIEW t_v AS SELECT a, b AS a FROM t;
contains:column "a" specified more than once

! CREATE VIEW t_v AS SELECT a, b FROM t AS t(a, a);
contains:column reference "a" is ambiguous

> CREATE VIEW t_v AS SELECT a, b FROM t;
> SELECT * FROM t_v;
1 2

> CREATE VIEW t_v_alias AS SELECT a, b FROM t AS t (b, a);
> SELECT * FROM t_v_alias
2 1

! CREATE MATERIALIZED VIEW t_m_v AS SELECT a, a FROM t;
contains:column "a" specified more than once

! CREATE MATERIALIZED VIEW t_m_v AS SELECT b, a, b FROM t;
contains:column "b" specified more than once

! CREATE MATERIALIZED VIEW t_m_v AS SELECT a, b AS a FROM t;
contains:column "a" specified more than once

! CREATE MATERIALIZED VIEW t_m_v AS SELECT a, b FROM t AS t(a, a);
contains:column reference "a" is ambiguous

> CREATE MATERIALIZED VIEW t_m_v AS SELECT a, b FROM t;
> SELECT * FROM t_v;
1 2

> CREATE MATERIALIZED VIEW t_m_v_alias AS SELECT a, b FROM t AS t (b, a);
> SELECT * FROM t_v_alias
2 1

# Regression for #9376
! CREATE VIEW gh9376 AS SELECT 1, 2;
contains:column "?column?" specified more than once

>CREATE VIEW gh9376 AS SELECT 1;
> SELECT name FROM mz_columns WHERE id = (SELECT id FROM mz_views WHERE name = 'gh9376');
\?column?
