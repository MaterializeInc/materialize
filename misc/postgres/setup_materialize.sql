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
CREATE DATABASE root OWNER root;
\c root
CREATE SCHEMA IF NOT EXISTS consensus OWNER root;
CREATE SCHEMA IF NOT EXISTS adapter OWNER root;
CREATE SCHEMA IF NOT EXISTS storage OWNER root;
CREATE SCHEMA IF NOT EXISTS tsoracle OWNER root;
-- Used in cargo test
GRANT ALL PRIVILEGES ON SCHEMA public TO root;
