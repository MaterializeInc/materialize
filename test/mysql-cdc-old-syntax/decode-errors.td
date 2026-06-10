# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Validate that decode errors are produced in the stream and can be
# retracted if the offending row(s) are deleted


> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

# Start with an invalid snapshot and verify the source presents an error
CREATE TABLE decode_err (f1 INTEGER, f2 TIME(6));

INSERT INTO decode_err VALUES (1, TIME '12:00:00');
INSERT INTO decode_err VALUES (2, TIME '29:59:59');

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR ALL TABLES;

! SELECT * FROM decode_err;
contains: error decoding value

# Remove the invalid row and verify that the source is now showing the correct data

$ mysql-execute name=mysql
DELETE FROM decode_err WHERE f1 = 2;

> SELECT * FROM decode_err;
1 "12:00:00"

# Re-add an invalid row and we verify that replication also correctly puts the source into an errored state

$ mysql-execute name=mysql
INSERT INTO decode_err VALUES (3, TIME '39:59:59');

! SELECT * FROM decode_err;
contains: error decoding value

# Remove the invalid row and the source is good again

$ mysql-execute name=mysql
DELETE FROM decode_err WHERE f1 = 3;

> SELECT * FROM decode_err;
1 "12:00:00"
