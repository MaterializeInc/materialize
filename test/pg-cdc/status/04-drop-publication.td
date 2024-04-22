# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP PUBLICATION mz_source;
INSERT INTO t VALUES (1);

> SELECT name, status FROM mz_internal.mz_source_statuses;
pg_source           stalled
pg_source_progress  running
t                   ceased

> SELECT error ILIKE '%publication "mz_source" does not exist' FROM mz_internal.mz_source_statuses WHERE name = 'pg_source';
true

# Check that new status reflects error from source.
> SELECT error ILIKE '%publication "mz_source" does not exist' FROM mz_internal.mz_source_statuses WHERE name = 't';
true

# TODO: This should be made reliable without sleeping, #25479
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=2s

! SELECT * FROM t;
contains:publication "mz_source" does not exist

# Tests must exit with healthy sources
> DROP SOURCE pg_source CASCADE;
