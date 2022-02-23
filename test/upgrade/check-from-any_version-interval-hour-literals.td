# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# this can't be run if the version is > 0.20.1, as v0.20.1 is when the behavior changed, requiring the migration
# this test is checking
$ skip-if
select split_part(ltrim('${arg.upgrade-from-version}', 'v'), '.', 1)::int = 0 and split_part(ltrim('${arg.upgrade-from-version}', 'v'), '.', 2)::int >= 20
and (split_part(ltrim('${arg.upgrade-from-version}', 'v'), '.', 2)::int <> 20 or split_part(ltrim('${arg.upgrade-from-version}', 'v'), '.', 3)::int > 1);

> SELECT * FROM interval_hour;
"5 days 03:00:00"
