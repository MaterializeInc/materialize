# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Statuses have not changed
> SELECT true
    FROM mz_internal.mz_source_statuses
    WHERE
        name = 'pg_source' AND status = 'ceased'
            AND
        error = 'publication "mz_source" does not exist';
true

# Subsource does not have its status changed
> SELECT true
    FROM mz_internal.mz_source_statuses
    WHERE
        name = 't' AND status = 'ceased'
            AND
        error ILIKE 'incompatible schema change%';
true

# Drop the source + subsources because some tests expect
# all remaining sources at the end of the test to be
# healthy.
> DROP SOURCE pg_source;
