# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Point Materialize at the *standby*, not the primary. Slot creation and the
# initial snapshot both create logical slots on the standby, which block until
# the primary emits a standby-snapshot record -- the mzcompose workflow nudges
# the primary in the background while this runs.

> DROP SECRET IF EXISTS pgpass CASCADE;
> DROP CONNECTION IF EXISTS pg CASCADE;

> CREATE SECRET pgpass AS 'postgres'

> CREATE CONNECTION pg TO POSTGRES (
    HOST 'pg-standby',
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')

> CREATE TABLE t FROM SOURCE mz_source (REFERENCE t)
> CREATE TABLE small FROM SOURCE mz_source (REFERENCE small)
