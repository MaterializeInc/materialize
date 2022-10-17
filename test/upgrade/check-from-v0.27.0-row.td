# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT f1::text, pg_typeof(f1) FROM row_view;
"(1,a,42)" "record(f1: integer,f2: text,f3: numeric)"
