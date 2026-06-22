-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- mzcompose.py sets QA_CANARY_ICEBERG_GCP_SA_JSON to the service-account JSON
-- bytes in bytea hex form (`\x...`): CREATE SECRET stores bytea, and a raw-JSON
-- literal would fail bytea_in on the `\n` escapes in the private key.
CREATE SECRET gcp_service_account_key AS env_var('QA_CANARY_ICEBERG_GCP_SA_JSON');
