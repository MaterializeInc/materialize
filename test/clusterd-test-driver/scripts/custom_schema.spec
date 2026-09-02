# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Declare a custom schema, write both synthetic and explicit rows against it,
# index it, and count. Run locally:
#   SCRIPT=test/clusterd-test-driver/scripts/custom_schema.spec \
#     bin/pyactivate test/clusterd-test-driver/run-local.py
#
# A three-column relation: bigint key, boolean flag, nullable text label.
create-instance
----
ok

update-configuration
----
ok

initialization-complete
----
ok

define-schema name=events
  key bigint
  flag boolean
  label text nullable
----
ok

# 1000 synthetic rows at ts 0 (key runs 0..1000, flag alternates, label padded).
write-single-ts shard=ev schema=events ts=0 count=1000
----
wrote 1000

# Two explicit rows at ts 1, including a null label. upper becomes 2.
write-rows shard=ev schema=events ts=1
  1000 true alpha
  1001 false null
----
wrote 2

# Index keyed by the bigint key column; as_of 0, shard upper 2.
define-index source=2000 index=2001 shard=ev schema=events key=[0] as-of=0 upper=2
----
ok

schedule id=2001
----
ok

await-frontier id=2001 ts=2
----
ok

# 1000 synthetic + 2 explicit = 1002 rows.
count id=2001 ts=1
----
1002
