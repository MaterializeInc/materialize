# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ s3-create-bucket bucket=test

$ s3-put-object bucket=test key=short/a
a1
a2
a3

$ s3-put-object bucket=test key=short/b
b1
b2
b3

> CREATE SECRET s3_conn_secret_access_key AS '${testdrive.aws-secret-access-key}';

> CREATE CONNECTION s3_conn FOR AWS
  ACCESS KEY ID = '${testdrive.aws-access-key-id}',
  SECRET ACCESS KEY = SECRET s3_conn_secret_access_key,
  TOKEN = '${testdrive.aws-token}';

> CREATE SOURCE s3_all
  FROM S3 CONNECTION s3_conn
  DISCOVER OBJECTS USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  LEGACYWITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}'
  )
  FORMAT TEXT;

> SELECT * FROM s3_all
a1
a2
a3
b1
b2
b3

> CREATE SOURCE s3_glob_a
  FROM S3 CONNECTION s3_conn
  DISCOVER OBJECTS MATCHING '**/a' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  LEGACYWITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}'
  )
  FORMAT TEXT;

> SELECT * FROM s3_glob_a
a1
a2
a3

> CREATE SOURCE s3_just_a
  FROM S3 CONNECTION s3_conn
  DISCOVER OBJECTS MATCHING 'short/a' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  LEGACYWITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}'
  )
  FORMAT TEXT;

> SELECT * FROM s3_just_a
a1
a2
a3

$ s3-put-object bucket=test key=csv.csv
c,7
c,8
c,9

> CREATE SOURCE csv_example (name, counts)
  FROM S3 CONNECTION s3_conn
  DISCOVER OBJECTS MATCHING '*.csv' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  LEGACYWITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}'
  )
  FORMAT CSV WITH 2 COLUMNS;

> SELECT * FROM csv_example
c 7
c 8
c 9

> SELECT * FROM csv_example WHERE counts = '8';
c 8

$ s3-put-object bucket=test key=logs/2020/12/31/frontend.log
99.99.44.44 - - [12/31/2020:23:55:59] "GET /updates HTTP/1.1" 200 10020 "-" "Python/Requests_22"
80.91.33.133 - - [12/31/2020:23:55:02] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Rust/reqwest 0.3"
173.203.139.108 - - [12/31/2020:23:55:07] "GET /wpadmin HTTP/1.1" 404 332 "-" "Firefox 9000"
173.203.139.108 - - [12/31/2020:23:55:14] "GET /downloads/materialized HTTP/1.1" 404 334 "-" "Python/Requests_22"
99.99.44.44 - - [12/31/2020:23:55:01] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Python/Requests_22"
80.91.33.133 - - [12/31/2020:23:55:41] "GET /downloads/materialized HTTP/1.1" 304 0 "-" "Rust/reqwest 0.3"
37.26.93.214 - - [12/31/2020:23:55:52] "GET /updates HTTP/1.1" 200 3318 "-" "Go_1.1_package_http"

$ s3-put-object bucket=test key=logs/2021/01/01/frontend.log
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

> CREATE SOURCE frontend_logs
  FROM S3 CONNECTION s3_conn
  DISCOVER OBJECTS MATCHING 'logs/**/*.log' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  LEGACYWITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}'
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

$ s3-create-bucket bucket=other

$ s3-put-object bucket=other key=short/c
c1
c2
c3

> CREATE SOURCE s3_multi
  FROM S3 CONNECTION s3_conn
  DISCOVER OBJECTS MATCHING 'short/*'
  USING BUCKET SCAN 'testdrive-test-${testdrive.seed}', BUCKET SCAN 'testdrive-other-${testdrive.seed}'
  LEGACYWITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}'
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


# CSV with headers

$ s3-put-object bucket=test key=csv/a.csv
id,value
1,a
2,a

$ s3-put-object bucket=test key=csv/b.csv
id,value
3,b

$ s3-put-object bucket=test key=csv/c.csv
id,value
4,c
5,c

> CREATE SOURCE s3_csv_headers
  FROM S3 CONNECTION s3_conn
  DISCOVER OBJECTS MATCHING 'csv/*'
  USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  LEGACYWITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}'
  )
  FORMAT CSV WITH HEADER (id, value);

> SELECT id, value FROM s3_csv_headers
id value
--------
1  a
2  a
3  b
4  c
5  c
