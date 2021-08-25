# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SHOW CREATE VIEW json_view;
"materialize.public.json_view" "CREATE VIEW \"materialize\".\"public\".\"json_view\" AS SELECT \"a\" -> 1, \"a\" ->> \'b\', \"a\" #> \'{b,1}\', \"a\" #>> \'{b, 1}\', \"a\" - \'b\', \"a\" @> \'{b, 1}\', \"a\" <@ \'{b, 1}\'::\"pg_catalog\".\"jsonb\", \"a\" ? \'b\' FROM \"materialize\".\"public\".\"json_table\""
