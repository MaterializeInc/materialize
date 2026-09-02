-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- A bare boolean on its own, on a cluster that keeps the default introspection
-- interval. Without an interval the server would reject the debugging flag.
CREATE CLUSTER implied_debugging (
    SIZE = 'scale=1,workers=1',
    INTROSPECTION DEBUGGING
);
