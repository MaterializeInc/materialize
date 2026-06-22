-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE TABLE product_category (
    category_id INT,
    category_name VARCHAR(255) NOT NULL,
    category_description TEXT
);

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE qa_canary_environment.public_loadgen_sources.product_category TO "infra+qacanaryload@materialize.io";
