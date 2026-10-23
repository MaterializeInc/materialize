-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Transfers from or to a flagged account. The correlation is an OR of two
-- equalities, which the optimizer decorrelates into a cross join.
CREATE MATERIALIZED VIEW flagged_transfers AS
SELECT t.*
FROM transfers t
WHERE EXISTS (
    SELECT 1 FROM flagged f WHERE f.account = t.src OR f.account = t.dst
);
