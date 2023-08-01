# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Check the various attributes supported by CREATE TABLE
#

> INSERT INTO "space table" VALUES (123);

> SELECT * FROM "space table";
123

> DROP TABLE "space table";

> INSERT INTO space_column VALUES (123);

> SELECT * FROM space_column
123

> DROP TABLE space_column;

> INSERT INTO default_value DEFAULT VALUES;

> SELECT * FROM default_value
123 "default value"

> SHOW COLUMNS IN not_null
f1 false integer

! INSERT INTO not_null VALUES (null)
contains:null value in column "f1" violates not-null constraint
