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

$ set queue-name=materialize-ci-notifications-${testdrive.seed}

$ s3-add-notifications bucket=${bucket} queue=${queue-name}

> CREATE MATERIALIZED SOURCE s3_all
  FROM S3 OBJECTS FROM SCAN BUCKET '${bucket}'
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
  FROM S3 OBJECTS FROM SCAN BUCKET '${bucket}' MATCHING '**/a'
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
  FROM S3 OBJECTS FROM SCAN BUCKET '${bucket}' MATCHING 'short/a'
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
  FROM S3 OBJECTS FROM SCAN BUCKET '${bucket}' MATCHING '*.csv'
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

$ s3-put-object bucket=${bucket} key=logs/2020/12/31/frontend.log
99.99.44.44 - - [12/31/2020:23:55:59] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests_22"
80.91.33.133 - - [12/31/2020:23:55:02] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Rust/reqwest 0.3"
173.203.139.108 - - [12/31/2020:23:55:07] "GET /wpadmin HTTP/1.1" 404 332 "-" "Firefox 9000"
173.203.139.108 - - [12/31/2020:23:55:14] "GET /downloads/materialized HTTP/1.1" 404 334 "-" "Python/Requests_22"
99.99.44.44 - - [12/31/2020:23:55:01] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Python/Requests_22"
80.91.33.133 - - [12/31/2020:23:55:41] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Rust/reqwest 0.3"
37.26.93.214 - - [12/31/2020:23:55:52] "GET /updates HTTP/1.1" 200 3318 "-" "Go_1.1_package_http"

$ s3-put-object bucket=${bucket} key=logs/2021/01/01/frontend.log
99.99.44.44 - - [01/01/2021:00:00:41] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests_22"
188.138.60.101 - - [01/01/2021:00:00:48] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests_22"
46.50.21.83 - - [01/01/2021:00:00:02] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Python/Requests_22.01"
99.99.44.44 - - [01/01/2021:00:00:25] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Python/Requests_22"
91.239.186.133 - - [01/01/2021:00:00:04] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests_22"
173.203.139.108 - - [01/01/2021:00:00:08] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Python/Requests_22"
80.91.33.133 - - [01/01/2021:00:00:04] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Rust/reqwest 0.3"
93.190.71.150 - - [01/01/2021:00:00:33] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests_22"
91.234.194.89 - - [01/01/2021:00:00:57] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests_22"
46.4.83.163 - - [01/01/2021:00:00:20] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Python/Requests_22"
173.203.139.108 - - [01/01/2021:00:00:39] "GET /downloads/materialized HTTP/1.1" 404 335 "-" "Python/Requests_22"

> CREATE MATERIALIZED SOURCE frontend_logs
  FROM S3 OBJECTS FROM SCAN BUCKET '${bucket}' MATCHING 'logs/**/*.log'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT REGEX '(?P<ip>[^ ]+) - - \[(?P<dt>[^]]+)\] "(?P<method>\w+) (?P<path>[^ ]+)[^"]+" (?P<status>\d+) (?P<content_length>\d+) "-" "(?P<user_agent>[^"]+)"';

> SELECT dt, ip, user_agent FROM frontend_logs WHERE path = '/updates' ORDER BY dt
01/01/2021:00:00:04 91.239.186.133 Python/Requests_22
01/01/2021:00:00:33 93.190.71.150 Python/Requests_22
01/01/2021:00:00:41 99.99.44.44 Python/Requests_22
01/01/2021:00:00:48 188.138.60.101 Python/Requests_22
01/01/2021:00:00:57 91.234.194.89 Python/Requests_22
12/31/2020:23:55:52 37.26.93.214 Go_1.1_package_http
12/31/2020:23:55:59 99.99.44.44 Python/Requests_22

$ set otherbucket=materialize-ci-td-other-${testdrive.seed}
$ set otherqueue=materialize-ci-other-${testdrive.seed}

$ s3-create-bucket bucket=${otherbucket}
$ s3-add-notifications bucket=${otherbucket} queue=${otherqueue} sqs-validation-timout=5m

$ s3-put-object bucket=${otherbucket} key=short/c
c1
c2
c3

> CREATE MATERIALIZED SOURCE s3_multi
  FROM S3 OBJECTS FROM SCAN BUCKET '${bucket}', SCAN BUCKET '${otherbucket}'
  MATCHING 'short/*'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT TEXT;

> SELECT text FROM s3_multi ORDER BY text;
a1
a2
a3
b1
b2
b3
c1
c2
c3

# Check a source that has both SCAN and NOTIFICATIONS set up

> CREATE MATERIALIZED SOURCE s3_scan_notifications
  FROM S3 OBJECTS FROM SCAN BUCKET '${bucket}', SQS NOTIFICATIONS '${queue-name}'
  MATCHING 'short/*'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT TEXT;

> SELECT text FROM s3_scan_notifications ORDER BY text;
a1
a2
a3
b1
b2
b3

$ s3-put-object bucket=${bucket} key=short/d
d

# S3 has a very loose idea of how fast items should show up:
# > Typically, event notifications are delivered in seconds but can sometimes take a minute or longer.
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html
$ set-sql-timeout duration=2minutes

> SELECT text FROM s3_scan_notifications ORDER BY text;
a1
a2
a3
b1
b2
b3
d

# ensure that only the source we care about gets the notifications
> DROP SOURCE s3_scan_notifications

# check just pulling SQS

# This section fails in localstack, but succeeds in AWS/CI
$ s3-put-object bucket=${otherbucket} key=short/e
e1
e2
e3

> CREATE MATERIALIZED SOURCE s3_notifications
  FROM S3 OBJECTS FROM SQS NOTIFICATIONS '${otherqueue}'
  MATCHING 'short/e'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT TEXT;

> SELECT text FROM s3_notifications ORDER BY text;
e1
e2
e3
