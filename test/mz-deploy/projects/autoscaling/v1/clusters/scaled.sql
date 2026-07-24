-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE CLUSTER scaled (
    SIZE = 'scale=1,workers=1',
    REPLICATION FACTOR = 1,
    AUTO SCALING STRATEGY = (
        ON HYDRATION (HYDRATION SIZE = 'scale=1,workers=2', LINGER DURATION = '60s')
    )
);
