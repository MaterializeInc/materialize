# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SHOW CREATE VIEW regexp_view;
"materialize.public.regexp_view" "CREATE VIEW \"materialize\".\"public\".\"regexp_view\" AS SELECT \"a\" !~~ \'b\', \"a\" ~~* \'b\', \"a\" ~ \'b\', \"a\" ~* \'b\', \"a\" !~ \'b\', \"a\" !~* \'b\' FROM \"materialize\".\"public\".\"regexp_table\""
