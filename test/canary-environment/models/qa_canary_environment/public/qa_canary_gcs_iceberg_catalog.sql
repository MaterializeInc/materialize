-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE CONNECTION qa_canary_gcs_iceberg_catalog TO ICEBERG CATALOG (
    CATALOG TYPE = 'rest',
    URL = 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
    WAREHOUSE = :'gcs_warehouse',
    GCP CONNECTION qa_canary_gcp_connection
);
