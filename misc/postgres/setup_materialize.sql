-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Sets up a Postgres DB for use by Materialize.

CREATE ROLE root WITH LOGIN PASSWORD 'root';
CREATE DATABASE root;
GRANT ALL PRIVILEGES ON DATABASE root TO root;
\c root
CREATE SCHEMA IF NOT EXISTS consensus AUTHORIZATION root;
CREATE SCHEMA IF NOT EXISTS adapter AUTHORIZATION root;
CREATE SCHEMA IF NOT EXISTS storage AUTHORIZATION root;
CREATE SCHEMA IF NOT EXISTS tsoracle AUTHORIZATION root;
-- Used in cargo test
GRANT ALL PRIVILEGES ON SCHEMA public TO root;
