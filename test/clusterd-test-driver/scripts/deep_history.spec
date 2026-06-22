# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# deep-history scenario: hydrate an index over a shard with many distinct
# timestamps. Rows are spread across 32 timestamps in a single append, and the
# index is built with as_of 0, so hydration must replay all 32 timestamps rather
# than read a single compacted snapshot.
create-instance
----
ok

update-configuration
----
ok

initialization-complete
----
ok

write-spread shard=dh count=5000 n-ts=32
----
wrote 5000

define-index source=1000 index=1001 shard=dh key=[0] as-of=0 upper=32
----
ok

schedule id=1001
----
ok

await-frontier id=1001 ts=32
----
ok

# Count at the last written timestamp (31); all 5000 rows must be present.
count id=1001 ts=31
----
5000
