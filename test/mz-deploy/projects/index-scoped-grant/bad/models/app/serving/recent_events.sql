-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE VIEW recent_events AS SELECT 1 AS id;

CREATE INDEX recent_events_idx IN CLUSTER serving ON recent_events (id);

GRANT SELECT ON recent_events TO PUBLIC USING INDEX other_idx;
