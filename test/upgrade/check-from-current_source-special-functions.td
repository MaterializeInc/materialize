# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that mz_logical_timestamp and other functions with special
# handling whose code has been refactored repeatedly are not mangled
# during a restart
#

> SHOW CREATE MATERIALIZED VIEW special_functions_view;
"materialize.public.special_functions_view" "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"special_functions_view\" IN CLUSTER \"default\" AS SELECT * FROM \"materialize\".\"public\".\"special_functions\" WHERE \"mz_catalog\".\"mz_logical_timestamp\"() > \"f1\""
