
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# It might be more interesting to have 1000 events at different times instead of all at the same timestamp
$ kafka-ingest topic=input format=bytes repeat=1000
"${kafka-ingest.iteration}"

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO table_pg SELECT generate_series(0, 1000);

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}
$ mysql-execute name=mysql
USE public;
INSERT INTO table_mysql SELECT 1 FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 1000;
