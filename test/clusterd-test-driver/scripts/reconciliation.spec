# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# reconciliation scenario: a reconnecting controller replays the dataflows it
# expects the replica to be running, and the replica reconciles them against its
# live dataflows rather than rehydrating from scratch.
#
# Load, index, hydrate.
create-instance
----
ok

update-configuration
----
ok

initialization-complete
----
ok

write-single-ts shard=data ts=0 count=5000
----
wrote 5000

define-index source=1000 index=1001 shard=data key=[0] as-of=0 upper=1
----
ok

schedule id=1001
----
ok

await-frontier id=1001 ts=1
----
ok

count id=1001 ts=0
----
5000

# Drop the connection and reconnect; only `Hello` is sent, so re-open the
# instance to start a new reconciliation window.
reconnect
----
ok

create-instance
----
ok

# Replay the identical dataflow (same ids, shard, key, as_of) as the reconciliation
# set, then close the window. The replica keeps the matching running dataflow.
define-index source=1000 index=1001 shard=data key=[0] as-of=0 upper=1
----
ok

schedule id=1001
----
ok

initialization-complete
----
ok

# The index is still serving on the new connection; the same rows are present.
await-frontier id=1001 ts=1
----
ok

count id=1001 ts=0
----
5000
