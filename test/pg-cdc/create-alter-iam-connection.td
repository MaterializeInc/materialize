# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests planning of AWS IAM authentication for PostgreSQL connections. These
# connections are not validated: there is no Amazon RDS instance to
# authenticate against.

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION aws_conn TO AWS (
    ENDPOINT "https://localhost:8080",
    REGION "us-east-1",
    ASSUME ROLE ARN "arn:aws:iam::123456789012:user/johndoe"
    );

> CREATE CONNECTION pg_iam_conn TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    AWS CONNECTION aws_conn,
    SSL MODE require
  ) WITH (VALIDATE false);

> SELECT name, type from mz_connections WHERE name IN ('aws_conn', 'pg_iam_conn') ORDER BY name
aws_conn aws
pg_iam_conn postgres

! ALTER CONNECTION pg_iam_conn SET (PASSWORD SECRET pgpass);
contains:IAM authentication is not supported with password

! ALTER CONNECTION pg_iam_conn DROP (SSL MODE);
contains:IAM authentication requires SSL to be enabled

> DROP CONNECTION pg_iam_conn;

#
# Error checking
#

! CREATE CONNECTION iam_or_password_not_both TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass,
    SSL MODE require,
    AWS CONNECTION aws_conn
    );
contains:IAM authentication is not supported with password

! CREATE CONNECTION iam_requires_ssl TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    AWS CONNECTION aws_conn
    );
contains:IAM authentication requires SSL to be enabled

! CREATE CONNECTION iam_requires_ssl TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    SSL MODE disable,
    AWS CONNECTION aws_conn
    );
contains:IAM authentication requires SSL to be enabled

> DROP CONNECTION aws_conn;
> DROP SECRET pgpass;
