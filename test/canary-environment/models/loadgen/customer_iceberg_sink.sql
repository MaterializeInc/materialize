-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

{{ config(materialized='sink', cluster='qa_canary_environment_storage') }}
FROM {{ ref('customer_tbl') }}
INTO ICEBERG CATALOG CONNECTION qa_canary_iceberg_catalog (NAMESPACE = 'qa_canary_environment', TABLE = 'customer')
USING AWS CONNECTION qa_canary_aws_connection
KEY (key)
MODE UPSERT
WITH (COMMIT INTERVAL = '60s');
