-- Copyright Materialize, Inc. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Enable replication, following the example from GitHub, with extra data removed
-- https://github.com/debezium/docker-images/blob/ecf6c2c2827a77117991ecedf5e81b2ec9418f53/examples/mysql/1.5/inventory.sql

GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicator' IDENTIFIED BY 'replpass';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT  ON *.* TO 'debezium' IDENTIFIED BY 'dbz';
