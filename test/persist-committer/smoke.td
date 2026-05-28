# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=30s

> DROP TABLE IF EXISTS committer_t;
> DROP MATERIALIZED VIEW IF EXISTS committer_mv;

> CREATE TABLE committer_t (a int);
> INSERT INTO committer_t VALUES (1), (2), (3);

> CREATE MATERIALIZED VIEW committer_mv AS SELECT count(*) FROM committer_t;

> SELECT * FROM committer_mv;
3

> INSERT INTO committer_t VALUES (4);

> SELECT * FROM committer_mv;
4
