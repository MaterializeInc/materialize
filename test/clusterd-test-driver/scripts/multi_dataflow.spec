# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# multi-dataflow scenario: attempt to hydrate two independent dataflows against
# one replica. This is a reproduction of a current limitation — often only one
# index hydrates. The awaits use `allow-timeout`, which emits a fixed `awaited`
# token regardless of outcome, so the golden output stays deterministic; this
# documents the limitation without asserting it is fixed.
create-instance
----
ok

update-configuration
----
ok

initialization-complete
----
ok

write-single-ts shard=ma ts=0 count=1000
----
wrote 1000

write-single-ts shard=mb ts=0 count=1000
----
wrote 1000

define-index source=1000 index=1001 shard=ma key=[0] as-of=0 upper=1
----
ok

schedule id=1001
----
ok

define-index source=1002 index=1003 shard=mb key=[0] as-of=0 upper=1
----
ok

schedule id=1003
----
ok

await-frontier id=1001 ts=1 timeout-secs=30 allow-timeout
----
awaited

await-frontier id=1003 ts=1 timeout-secs=30 allow-timeout
----
awaited
