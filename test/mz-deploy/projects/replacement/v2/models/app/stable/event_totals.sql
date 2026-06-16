-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE MATERIALIZED VIEW event_totals
    IN CLUSTER compute
    AS
    SELECT category, COUNT(*) AS event_count
    FROM app.public.events
    WHERE amount IS NOT NULL
    GROUP BY category;
