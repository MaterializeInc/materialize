-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

COMMENT ON SCHEMA app.ops IS 'operational tables';

GRANT USAGE ON SCHEMA app.ops TO monitor_user;

ALTER DEFAULT PRIVILEGES FOR ROLE deploy_user IN SCHEMA app.ops
    GRANT SELECT ON TABLES TO materialize;

ALTER DEFAULT PRIVILEGES FOR ROLE deploy_user IN SCHEMA app.ops
    GRANT USAGE ON SECRETS TO monitor_user;
