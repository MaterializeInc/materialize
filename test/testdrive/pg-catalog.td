# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SHOW COLUMNS FROM pg_namespace
Field    Nullable  Type
--------------------------
oid      NO        oid
nspname  NO        text
nspowner YES       oid

> SHOW COLUMNS FROM pg_class
Field        Nullable  Type
--------------------------
oid          NO        oid
relname      NO        text
relnamespace NO        oid
relowner     YES       oid
