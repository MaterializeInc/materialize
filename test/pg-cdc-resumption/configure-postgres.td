# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE USER debezium WITH SUPERUSER PASSWORD 'debezium';
GRANT ALL PRIVILEGES ON DATABASE "postgres" TO debezium;
GRANT ALL PRIVILEGES ON SCHEMA "public" TO debezium;

CREATE PUBLICATION mz_source;
CREATE PUBLICATION mz_source_alter;
