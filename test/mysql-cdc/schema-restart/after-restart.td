# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


> SELECT true
    FROM mz_internal.mz_source_statuses
    WHERE
        name = 'schema_test' AND status = 'running';
true

# TODO: redesign ceased status #25768
# # dummy subsource is put into error state
# > SELECT true
#     FROM mz_internal.mz_source_statuses
#     WHERE
#         name = 'dummy' AND status = 'ceased'
#             AND
#         error ILIKE 'incompatible schema change%';
# true

# other table still has data
> SELECT count(*) FROM other;
3

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
USE public
INSERT INTO other VALUES (4), (5);

> SELECT count(*) FROM OTHER;
5

# Drop the source + subsources because some tests expect
# all remaining sources at the end of the test to be
# healthy.
> DROP SOURCE schema_test CASCADE;
