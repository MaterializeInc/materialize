# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE USER materialize PASSWORD 'materialize';
ALTER USER materialize REPLICATION;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO materialize;

CREATE PUBLICATION mz_source FOR ALL TABLES;
