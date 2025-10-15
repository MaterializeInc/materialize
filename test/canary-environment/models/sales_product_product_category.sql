-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.
-- depends_on: {{ ref('sales_tbl') }}

{{ config(materialized='materialized_view', cluster="qa_canary_environment_compute", indexes=[{'default': True}]) }}

SELECT
    count(*) AS count_star,
    count(distinct sales_tbl.key) As count_distinct_loadgen_sales_key,
    count(distinct product_id) AS count_distinct_product_id,
    count(distinct category_id) AS count_distinct_category_id
FROM      {{ source('loadgen','sales_tbl') }}
LEFT JOIN public_loadgen.product USING (product_id)
LEFT JOIN public_loadgen.product_category USING (category_id)
