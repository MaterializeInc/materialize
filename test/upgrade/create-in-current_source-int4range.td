# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# CREATE a view containing int4range

> CREATE MATERIALIZED VIEW int4range_view (a) AS
  SELECT column1::int4range FROM
    (
        VALUES
        ('[,1)'),
        ('[,1]'),
        ('[,)'),
        ('[,]'),
        ('(,1)'),
        ('(,1]'),
        ('(,)'),
        ('(,]'),
        ('[-1,1)'),
        ('[-1,1]'),
        ('(-1,1)'),
        ('(-1,1]'),
        ('[0,0)'),
        ('[0,0]'),
        ('(0,0)'),
        ('(0,0]'),
        ('[1,)'),
        ('[1,]'),
        ('(1,)'),
        ('(1,]')
    );

> SELECT a::text AS t FROM int4range_view ORDER BY a;
empty
empty
empty
(,1)
(,1)
(,2)
(,2)
(,)
(,)
(,)
(,)
[-1,1)
[-1,2)
[0,1)
[0,1)
[0,2)
[1,)
[1,)
[2,)
[2,)
