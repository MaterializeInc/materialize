-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Only SIZE. Every other option `SHOW CREATE CLUSTER` renders holds its
-- server default, which reconciliation must not read as drift.
CREATE CLUSTER sized SIZE = 'scale=1,workers=1';
