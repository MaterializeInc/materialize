-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Three spellings the server rewrites before it records them: a bare boolean
-- carries its implied `true`, a zero introspection interval disables
-- introspection and comes back as NULL, and DISK is a no-op the server drops.
-- A legacy size, because DISK is rejected outright on a `cc` one.
CREATE CLUSTER zero_and_noops (
    SIZE = 'scale=1,workers=1,legacy',
    DISK,
    EXPERIMENTAL ARRANGEMENT COMPRESSION,
    INTROSPECTION INTERVAL = 0,
    MANAGED
);
