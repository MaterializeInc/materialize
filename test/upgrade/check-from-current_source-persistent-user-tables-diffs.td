# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# If mz_persist::gen::ProtoMeta::ENCODING_VERSION is changed, the version of this file needs to change to "current_source"

# Generate rows where diffs do not fit in a 32-bit integer

> SELECT COUNT(*) = pow(2, 33)::bigint FROM persistent_user_table_large_positive_diff;
true

! SELECT * FROM persistent_user_table_negative_diff;
contains:Invalid data in source, saw retractions (1) for row that does not exist: [Null]

! SELECT * FROM persistent_user_table_large_negative_diff;
contains:Invalid data in source, saw retractions (8589934592) for row that does not exist: [Null]

> SELECT COUNT(*) = 0 FROM persistent_user_table_cancelling_diffs;
true
