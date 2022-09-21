# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT mz_uptime() > interval '1 second' AND mz_uptime() < interval '1 year';
true

> SELECT mz_version() != '';
true

> SELECT current_user = current_role AND current_user != '';
true

> SELECT session_user = current_role AND session_user != '';
true

> SELECT now() - pg_postmaster_start_time() > interval '1 second' AND now() - pg_postmaster_start_time() < interval '1 year';
true

> SELECT length(cast(mz_environment_id() as text));
50

> SELECT length(cast(mz_internal.mz_session_id() as text));
36
