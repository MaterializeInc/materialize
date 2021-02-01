# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set bucket=materialize-ci-testdrive-${testdrive.seed}

$ s3-create-bucket bucket=${bucket}

$ s3-put-object bucket=${bucket} key=short/a
a1
a2
a3

$ s3-put-object bucket=${bucket} key=short/b
b1
b2
b3

> CREATE MATERIALIZED SOURCE s3_all
  FROM S3 BUCKET '${bucket}' OBJECTS FROM SCAN
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT TEXT;

> SELECT * FROM s3_all ORDER BY mz_record;
a1 1
a2 2
a3 3
b1 4
b2 5
b3 6

> CREATE MATERIALIZED SOURCE s3_glob_a
  FROM S3 BUCKET '${bucket}' OBJECTS FROM SCAN MATCHING '**/a'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT TEXT;

> SELECT * FROM s3_glob_a ORDER BY mz_record;
a1 1
a2 2
a3 3

> CREATE MATERIALIZED SOURCE s3_just_a
  FROM S3 BUCKET '${bucket}' OBJECTS FROM SCAN MATCHING 'short/a'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT TEXT;

> SELECT * FROM s3_just_a ORDER BY mz_record;
a1 1
a2 2
a3 3

$ s3-put-object bucket=${bucket} key=csv.csv
c,7
c,8
c,9

> CREATE MATERIALIZED SOURCE csv_example (name, counts)
  FROM S3 BUCKET '${bucket}' OBJECTS FROM SCAN MATCHING '*.csv'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT CSV WITH 2 COLUMNS;

> SELECT * FROM csv_example ORDER BY mz_record;
c 7 1
c 8 2
c 9 3

> SELECT * FROM csv_example WHERE counts = '8';
c 8 2

$ s3-put-object bucket=${bucket} key=server.log
[2020-01-20T12:00:00] INFO 10.0.0.1 GET /login connection_established
[2020-01-20T12:00:01] INFO 10.0.0.1 PUT /login logged_in

> CREATE MATERIALIZED SOURCE re
  FROM S3 BUCKET '${bucket}' OBJECTS FROM SCAN MATCHING '*.log'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT REGEX '\[(?P<dt>[^]]+)\] (?P<level>\w+) (?P<ip>[^ ]+) (?P<method>\w+) (?P<path>[^ ]+) (?P<message>.*)';

> SELECT * FROM re ORDER BY dt DESC
2020-01-20T12:00:01 INFO 10.0.0.1 PUT /login logged_in 2
2020-01-20T12:00:00 INFO 10.0.0.1 GET /login connection_established 1

> SELECT * FROM re WHERE method = 'PUT'
2020-01-20T12:00:01 INFO 10.0.0.1 PUT /login logged_in 2
