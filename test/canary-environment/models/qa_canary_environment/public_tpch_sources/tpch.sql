-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE SOURCE tpch
    IN CLUSTER qa_canary_environment_storage
    FROM LOAD GENERATOR TPCH (SCALE FACTOR 1, TICK INTERVAL '0.1s');

GRANT ALL PRIVILEGES ON TABLE qa_canary_environment.public_tpch_sources.tpch TO "infra+bot@materialize.com", "infra+qacanaryload@materialize.io";
