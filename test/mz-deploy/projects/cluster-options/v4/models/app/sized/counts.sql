-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Puts an object on `sized` so that `stage` has a reason to clone the cluster.
CREATE MATERIALIZED VIEW counts
    IN CLUSTER sized
    AS
    SELECT id, COUNT(*) AS n
    FROM app.public.marker
    GROUP BY id;
