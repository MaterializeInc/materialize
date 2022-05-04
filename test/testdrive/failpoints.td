# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Basic sanity checks around the SET failpoints syntax
#

! SET failpoints = 'a';
contains:parameter "failpoints" cannot have value "a": missing failpoint action

! SET failpoints = 'a=1';
contains:parameter "failpoints" cannot have value "a=1": unrecognized command "1"

> SET failpoints = 'a=off';

#
# Set and activate a failpoint that is local to the coordinator
#

> SELECT mz_internal.mz_set_failpoints('coordinator${testdrive.seed}=return');
coordinator${testdrive.seed}=return

! SELECT mz_internal.mz_failpoint('coordinator${testdrive.seed}')
contains: failpoint coordinator${testdrive.seed} returned

> SELECT mz_internal.mz_set_failpoints('coordinator${testdrive.seed}=off');
coordinator${testdrive.seed}=off

> SELECT mz_internal.mz_failpoint('coordinator${testdrive.seed}')
coordinator${testdrive.seed}

#
# Set and activate a failpoint in the dataflow
#

> CREATE TABLE set_failpoints(failpoints TEXT);

> CREATE MATERIALIZED VIEW set_failpoints_v AS SELECT mz_internal.mz_set_failpoints(failpoints) FROM set_failpoints;

> CREATE TABLE failpoints(failpoint TEXT);

> CREATE MATERIALIZED VIEW failpoints_v AS SELECT mz_internal.mz_failpoint(failpoint) FROM failpoints;

> INSERT INTO set_failpoints VALUES ('dataflow${testdrive.seed}=return')

> INSERT INTO failpoints VALUES ('dataflow${testdrive.seed}');

! SELECT * FROM failpoints_v;
contains: failpoint dataflow${testdrive.seed} returned

> INSERT INTO set_failpoints VALUES ('dataflow${testdrive.seed}=off')
