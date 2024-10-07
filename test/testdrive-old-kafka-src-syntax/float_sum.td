# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test the implementation of SUM over floats
#

> CREATE TABLE "nan" (f1 float);

> INSERT INTO "nan" VALUES ('NaN');

> SELECT SUM(f1) FROM "nan";
NaN

> CREATE TABLE "neg_infinity" (f1 float);

> INSERT INTO "neg_infinity" VALUES ('-infinity');

> SELECT SUM(f1) FROM "neg_infinity";
-inf

> CREATE TABLE a (f1 float);

> INSERT INTO a VALUES ('infinity');

> SELECT SUM(f1) FROM a;
inf

> INSERT INTO a VALUES (5.12);

> SELECT SUM(f1) FROM a;
inf

> INSERT INTO a VALUES ('-infinity');

> SELECT SUM(f1) FROM a;
NaN

> DELETE FROM a WHERE f1 <> 5.12;

# TODO(petrosagg): this is a correctness issue that is a consequence of mapping
#                  float values onto a i128 domain to do the aggregation
> SELECT SUM(f1) FROM a;
5.119999945163727

> DELETE FROM a;

> SELECT SUM(f1) FROM a;
<null>

> INSERT INTO a VALUES (NULL);

> SELECT SUM(f1) FROM a;
<null>
