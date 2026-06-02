# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Data round-trip for the CHANGES(<collection> AS OF <bound>) table function.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_changes_table_function = true

> CREATE TABLE t (a int)

> INSERT INTO t VALUES (1), (2)

# Capture a valid changelog start `as_of`: the current logical time, at which
# the table's contents are {1, 2}.
$ set-from-sql var=as-of
SELECT mz_now()::text

# These changes happen strictly after `as_of`, so they appear as appends rather
# than as part of the snapshot.
> INSERT INTO t VALUES (3)

> DELETE FROM t WHERE a = 1

# The changelog is: the snapshot at `as_of` ({1, 2}) collapsed to `as_of`, then
# the later insert of 3 and deletion of 1, appended at the times they occurred.
# The per-update timestamp is non-deterministic, so we assert only the data and
# `mz_diff` columns. The changelog is append-only: every change is materialized
# as an insert (output diff +1), so the deletion of 1 shows up as a row with
# mz_diff = -1 rather than as a retraction.
> SELECT a, mz_diff FROM CHANGES (t AS OF ${as-of}) ORDER BY a, mz_diff
1 -1
1 1
2 1
3 1

# The changelog has one row per change: two snapshot rows plus two appends.
> SELECT count(*) FROM CHANGES (t AS OF ${as-of})
4

# CHANGES composes with ordinary SQL: aggregate the net change per key.
> SELECT a, sum(mz_diff) FROM CHANGES (t AS OF ${as-of}) GROUP BY a ORDER BY a
1 0
2 1
3 1

# A sliding (mz_now()-relative) bound: mz_now() resolves to the query time, so a
# zero lag makes the changelog start "now" and the snapshot is the table's
# current contents ({2, 3} after the insert of 3 and deletion of 1), each as a
# +1 append.
> SELECT a, mz_diff FROM CHANGES (t AS OF mz_now()) ORDER BY a
2 1
3 1

# The relation may be a parenthesized subquery that reduces to a bare read of
# the collection (mirroring SUBSCRIBE); it behaves exactly like naming it.
> SELECT a, mz_diff FROM CHANGES ((SELECT * FROM t) AS OF ${as-of}) ORDER BY a, mz_diff
1 -1
1 1
2 1
3 1

> DROP TABLE t
