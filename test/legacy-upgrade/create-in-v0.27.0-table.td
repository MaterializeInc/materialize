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

> CREATE TABLE "space table" (f1 INTEGER);

> CREATE TABLE space_column ("space column" INTEGER);

> CREATE TABLE default_value (f1 INTEGER DEFAULT 123, f2 TEXT DEFAULT 'default value');

> CREATE TABLE not_null (f1 INTEGER NOT NULL);
