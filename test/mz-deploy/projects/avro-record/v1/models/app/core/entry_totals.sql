-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Reads through the record column, including a field the Avro decoder invented
-- by splitting a three-way union.
CREATE VIEW entry_totals AS
SELECT
    (record).currency AS currency,
    (record).type AS entry_type,
    sum((record).amount1) AS amount,
    sum((record).fee1) AS fee,
    max((record).created) AS last_seen
FROM app.ingest.ledger_entry
GROUP BY 1, 2;
