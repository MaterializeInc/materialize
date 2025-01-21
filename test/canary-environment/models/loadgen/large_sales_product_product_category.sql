-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.
-- depends_on: {{ ref('sales_large_tbl') }}
-- depends_on: {{ ref('product_tbl') }}
-- depends_on: {{ ref('product_category_tbl') }}

{{ config(materialized='materialized_view', cluster="qa_canary_environment_compute", indexes=[{'default': True}]) }}

SELECT
    count(*) AS count_star,
    count(distinct sales_large_tbl.key) As count_distinct_loadgen_sales_large_key,
    count(distinct product_id) AS count_distinct_product_id,
    count(distinct category_id) AS count_distinct_category_id
FROM      {{ source('loadgen','sales_large_tbl') }}
LEFT JOIN {{ source('loadgen','product_tbl') }} USING (product_id)
LEFT JOIN {{ source('loadgen','product_category_tbl') }} USING (category_id)
