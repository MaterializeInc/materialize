-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE MATERIALIZED VIEW sales_product_product_category
    IN CLUSTER qa_canary_environment_compute
    AS
    SELECT
        count(*) AS count_star,
        count(distinct sales_tbl.key) AS count_distinct_loadgen_sales_key,
        count(distinct product_id) AS count_distinct_product_id,
        count(distinct category_id) AS count_distinct_category_id
    FROM qa_canary_environment.public_loadgen_sources.sales_tbl
    LEFT JOIN qa_canary_environment.public_loadgen_sources.product USING (product_id)
    LEFT JOIN qa_canary_environment.public_loadgen_sources.product_category USING (category_id);

CREATE DEFAULT INDEX IN CLUSTER qa_canary_environment_compute ON sales_product_product_category;

GRANT ALL PRIVILEGES ON TABLE qa_canary_environment.public_loadgen.sales_product_product_category TO "infra+bot@materialize.com", "infra+qacanaryload@materialize.io";
