-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE TABLE product (
    product_id INT,
    category_id INT,
    product_name VARCHAR(255) NOT NULL,
    product_description TEXT,
    product_price DECIMAL(10, 2) NOT NULL
);

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE qa_canary_environment.public_loadgen_sources.product TO "infra+qacanaryload@materialize.io";
