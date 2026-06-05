-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE RECORDER foo_a_recorder WITH
  -- Driver: fires only when (key, a) changes. Joining foo back as a bare
  -- TVC reference freezes the current b at processing time.
  recorded AS (
    SELECT c.key, c.a, f.b, c.mz_timestamp, c.mz_diff
    FROM CHANGES(foo_ka AS OF AT LEAST 0) c
    JOIN foo f ON c.key = f.key
    WHERE c.mz_diff > 0
  ),
  -- Most recent recorded row per key.
  latest AS (
    SELECT DISTINCT ON (key) key, a, b
    FROM a_log
    WHERE mz_diff > 0
    ORDER BY key, mz_timestamp DESC
  ),
  -- Rows that are NOT the per-key latest: a newer recorded row exists.
  superseded AS (
    SELECT l.*
    FROM a_log l
    WHERE l.mz_diff > 0
      AND EXISTS (
        SELECT 1 FROM a_log l2
        WHERE l2.key = l.key
          AND l2.mz_diff > 0
          AND l2.mz_timestamp > l.mz_timestamp
      )
  )
AS
  RECORD    recorded   INTO a_log,             -- append frozen (key, a, b) deltas
  INTEGRATE latest     AS   foo_with_frozen_b, -- the maintained view you want
  DELETE    superseded FROM a_log;             -- bound the log: one row per key
