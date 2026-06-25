-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE SOURCE sales
    IN CLUSTER qa_canary_environment_upsert
    FROM KAFKA CONNECTION qa_canary_environment.public.kafka_connection (TOPIC 'qa_canary_sales');

GRANT ALL PRIVILEGES ON TABLE qa_canary_environment.public_loadgen_sources.sales TO "infra+bot@materialize.com", "infra+qacanaryload@materialize.io";
