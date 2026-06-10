# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Subsource does not have its status changed
> SELECT status = 'ceased' AND error ILIKE '%incompatible schema change%'
    FROM mz_internal.mz_source_statuses
    WHERE name = 't'
true

! SELECT * FROM t;
contains:incompatible schema change
