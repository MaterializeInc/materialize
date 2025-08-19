# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s


#
# Test the JSON data type
#

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

# Insert data pre-snapshot
CREATE TABLE t1 (f1 JSON);

INSERT INTO t1 VALUES (CAST('{"bar": "baz", "balance": 7.77, "active": false}' AS JSON));
INSERT INTO t1 VALUES (CAST('null' AS JSON));
INSERT INTO t1 VALUES (JSON_ARRAY('x', 1, NULL, CAST('{"x": ["y"]}' AS JSON)));
INSERT INTO t1 VALUES (NULL);
INSERT INTO t1 VALUES (JSON_ARRAY(NULL));

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1);
> COMMIT

> SELECT COUNT(*) > 0 FROM t1;
true

# Insert the same data post-snapshot
$ mysql-execute name=mysql
INSERT INTO t1 SELECT * FROM t1;

> SELECT pg_typeof(f1) FROM t1 LIMIT 1;
jsonb

> SELECT * FROM t1;
<null>
<null>
null
null
"{\"active\":false,\"balance\":7.77,\"bar\":\"baz\"}"
"{\"active\":false,\"balance\":7.77,\"bar\":\"baz\"}"
"[\"x\",1,null,{\"x\":[\"y\"]}]"
"[\"x\",1,null,{\"x\":[\"y\"]}]"
"[null]"
"[null]"

> SELECT f1->>'balance' FROM t1;
<null>
<null>
<null>
<null>
<null>
<null>
<null>
<null>
7.77
7.77

> SELECT count(*) FROM t1 WHERE f1 IS NULL;
2

> SELECT count(*) FROM t1 WHERE f1 = 'null'::jsonb;
2
