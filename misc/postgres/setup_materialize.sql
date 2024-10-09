-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Sets up a Postgres DB for use by Materialize.

CREATE ROLE materialize WITH LOGIN PASSWORD 'materialize';
CREATE DATABASE materialize;
GRANT ALL PRIVILEGES ON DATABASE materialize TO materialize;
\c materialize
CREATE SCHEMA IF NOT EXISTS consensus;
CREATE SCHEMA IF NOT EXISTS adapter;
CREATE SCHEMA IF NOT EXISTS storage;
CREATE SCHEMA IF NOT EXISTS tsoracle;
