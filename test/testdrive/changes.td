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

# CHANGES over a materialized view (a non-table persist-backed input). The
# changelog reads the MV's own shard; changes to the underlying table flow
# through to the MV's changelog.
> CREATE TABLE base (a int)

> INSERT INTO base VALUES (1), (2)

> CREATE MATERIALIZED VIEW mv AS SELECT a, a * 10 AS b FROM base

# Wait for the MV to reflect the base contents, then capture a changelog start
# at which the MV holds {(1, 10), (2, 20)}.
> SELECT a, b FROM mv ORDER BY a
1 10
2 20

$ set-from-sql var=mv-as-of
SELECT mz_now()::text

> INSERT INTO base VALUES (3)

# The MV changelog is the {(1, 10), (2, 20)} snapshot collapsed to `as_of` plus
# the (3, 30) row appended once the insert propagates. The net change per key
# (sum of mz_diff) is the MV's current contents; testdrive retries until the MV
# catches up.
> SELECT a, b, sum(mz_diff) FROM CHANGES (mv AS OF ${mv-as-of}) GROUP BY a, b ORDER BY a
1 10 1
2 20 1
3 30 1

> DROP MATERIALIZED VIEW mv

> DROP TABLE base

# Strict `AS OF <past>` reading back into retained history. A generous
# RETAIN HISTORY window keeps the input's `since` far behind its `upper`, so the
# fixed past bound is read exactly as written: neither clamped up to `since` (as
# an advisory `AS OF AT LEAST` bound would be) nor rejected (as a strict bound
# would be once compaction advanced `since` past it on a default-retention
# collection). This makes the look-back deterministic rather than racing
# compaction.
> CREATE TABLE h (a int) WITH (RETAIN HISTORY FOR '7 days')

> INSERT INTO h VALUES (1), (2)

# Capture a strict changelog start at which `h` holds {1, 2}.
$ set-from-sql var=h-as-of
SELECT mz_now()::text

# A burst of changes strictly after the captured bound, including an UPDATE
# (which is a retraction of the old row plus an insert of the new one).
> INSERT INTO h VALUES (3)

> DELETE FROM h WHERE a = 1

> UPDATE h SET a = 20 WHERE a = 2

# The strict changelog from the past bound: the {1, 2} snapshot collapsed to the
# bound, then every later change as an append. The 7-day retention guarantees
# `since` is still behind the bound, so this returns history rather than erroring.
> SELECT a, mz_diff FROM CHANGES (h AS OF ${h-as-of}) ORDER BY a, mz_diff
1 -1
1 1
2 -1
2 1
3 1
20 1

# Net change per key over the retained window: 1 and 2 churn to zero, 3 and 20
# remain — i.e. the current contents {3, 20}.
> SELECT a, sum(mz_diff) FROM CHANGES (h AS OF ${h-as-of}) GROUP BY a ORDER BY a
1 0
2 0
3 1
20 1

> DROP TABLE h

# A maintained materialized view over CHANGES with a sliding bound. The window
# trails mz_now() by the lag; the changelog ages in from creation (the input's
# `since`), so creating the MV over an empty table makes the contents
# deterministic: exactly the changes made after creation (all within the
# generous 1-hour window for the duration of this test).
> CREATE TABLE w (a int)

> CREATE MATERIALIZED VIEW w_changes AS
  SELECT a, mz_diff FROM CHANGES (w AS OF AT LEAST mz_now() - INTERVAL '1 hour')

# The window ages in empty: no changes yet.
> SELECT count(*) FROM w_changes
0

> INSERT INTO w VALUES (1), (2)

> INSERT INTO w VALUES (3)

> DELETE FROM w WHERE a = 1

# Every change since creation is materialized as an append: the deletion of 1
# shows up as a row with mz_diff = -1.
> SELECT a, mz_diff FROM w_changes ORDER BY a, mz_diff
1 -1
1 1
2 1
3 1

# Aggregations over the maintained changelog are bounded by the window and
# correct: the net change per key is the table's current contents.
> SELECT a, sum(mz_diff) FROM w_changes GROUP BY a ORDER BY a
1 0
2 1
3 1

> DROP MATERIALIZED VIEW w_changes

> DROP TABLE w
