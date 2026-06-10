# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

! INSERT INTO t VALUES (1, 'ghp_9fK8sL3x7TqR1vEzYm2pDaN4WjXbQzUtV0aN');
contains:unknown catalog item 't'

! SHOW CREATE TABLE t;
contains:unknown catalog item 't'

> CREATE TABLE t (a text NOT NULL)

> INSERT INTO t VALUES ('ghp_9fK8sL3x7TqR1vEzYm2pDaN4WjXbQzUtV0aN');

> SELECT * FROM t
ghp_9fK8sL3x7TqR1vEzYm2pDaN4WjXbQzUtV0aN

> UPDATE t SET a = 'ghp_9fK8sL3x7TqR1vEzYm2pDaN4WjXbQzUtV0aN'

> DELETE FROM t WHERE a = 'ghp_9fK8sL3x7TqR1vEzYm2pDaN4WjXbQzUtV0aN'

> CREATE VIEW v AS SELECT 'ghp_9fK8sL3x7TqR1vEzYm2pDaN4WjXbQzUtV0aN'

> CREATE MATERIALIZED VIEW mv AS SELECT 'ghp_9fK8sL3x7TqR1vEzYm2pDaN4WjXbQzUtV0aN'

> CREATE SECRET s AS 'ghp_9fK8sL3x7TqR1vEzYm2pDaN4WjXbQzUtV0aN'
