# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# peek_yielding scenario: serve an index peek under a work budget so small that
# the arrangement scan cannot finish in one worker activation, and check that
# resuming it returns the same collection a single-slice scan would.
#
# `work:1` lets the peek advance one cursor position per turn, so the peeks
# below resume on the order of 2000 times each.
#
# `peek-count` rather than `count`: `count` tallies through an ephemeral reduce
# dataflow and then peeks that dataflow's single-row output, so the peek itself
# would only ever walk one cursor position and none of this would be exercised.
# `peek-count` peeks the index directly.
#
# What this pins: no row is lost or duplicated across a yield, `Step::Done`
# stays sticky once the cursor is exhausted, a finished scan leaves nothing
# behind that corrupts the next peek, a peek spanning many activations still
# produces exactly one response, and the same holds for the literal-constraint
# path, whose cursor seeks rather than steps.
#
# What it does not pin. Row contents, since the golden is a count — that
# coverage comes from running sqllogictest with the small CI budgets, where the
# goldens compare rows positionally. And `peek_yielding_total`, because the
# driver awaits each peek before sending the next, so two peeks are never
# pending at once and the activation budget never binds.
create-instance
----
ok

update-configuration
peek_yielding string work:1,time:60000
----
ok

initialization-complete
----
ok

write-single-ts shard=data ts=0 count=2000
----
wrote 2000

define-index source=1000 index=1001 shard=data key=[0] as-of=0 upper=1
----
ok

schedule id=1001
----
ok

await-frontier id=1001 ts=1
----
ok

# The whole collection survives a scan that yielded after every cursor position.
peek-count id=1001 ts=0
----
2000

# Again, to check a completed scan left nothing behind for the next peek.
peek-count id=1001 ts=0
----
2000

# And the reduce path still works with peeks yielding underneath it.
count id=1001 ts=0
----
2000

# Literal constraints take a different path through the scan. The cursor seeks
# from one literal to the next instead of stepping, and the position in the
# literal list has to survive a yield just as the cursor position does. Keys run
# 0..1999.

# Unsorted literals, all matching. The replica sorts them itself.
peek-count id=1001 ts=0 literals=[1999,5,500]
----
3

# Literals matching no key are skipped. Each skip is a seek, and a run of them
# happens within one unit of fuel.
peek-count id=1001 ts=0 literals=[9999,5,2500,500,3000]
----
2

# A run of non-matching literals after the last match, so the scan finishes by
# exhausting the literal list rather than the cursor.
peek-count id=1001 ts=0 literals=[1999,2000,2001,2002,2003]
----
1

# Nothing matches at all.
peek-count id=1001 ts=0 literals=[3000,4000]
----
0

# An empty literal list selects nothing.
peek-count id=1001 ts=0 literals=[]
----
0

# Enough matching literals to yield repeatedly between them.
peek-count id=1001 ts=0 literals=[0,100,200,300,400,500,600,700,800,900,1000,1100,1200,1300,1400,1500,1600,1700,1800,1900]
----
20
