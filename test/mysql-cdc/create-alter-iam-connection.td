# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION aws_conn TO AWS (
    ENDPOINT "https://localhost:8080",
    REGION "us-east-1",
    ASSUME ROLE ARN "arn:aws:iam::123456789012:user/johndoe"
    );

> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    AWS CONNECTION aws_conn,
    SSL MODE required
  ) WITH (VALIDATE false);

> SELECT name, type from mz_connections WHERE id LIKE 'u%'
aws_conn aws
mysql_conn mysql

>[version<2600700] SHOW CREATE CONNECTION mysql_conn
materialize.public.mysql_conn "CREATE CONNECTION materialize.public.mysql_conn TO MYSQL (AWS CONNECTION = materialize.public.aws_conn, HOST = mysql, SSL MODE = required, USER = root);"

>[version>=2600700] SHOW CREATE CONNECTION mysql_conn
materialize.public.mysql_conn "CREATE CONNECTION materialize.public.mysql_conn\nTO MYSQL\n    (AWS CONNECTION = materialize.public.aws_conn, HOST = mysql, SSL MODE = required, USER = root);"


! ALTER CONNECTION mysql_conn SET (PASSWORD SECRET mysqlpass);
contains:IAM authentication is not supported with password

! ALTER CONNECTION mysql_conn DROP (SSL MODE);
contains:IAM authentication requires SSL to be enabled

> DROP CONNECTION mysql_conn;

#
# Error checking
#

! CREATE CONNECTION iam_or_password_not_both TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass,
    SSL MODE "required",
    AWS CONNECTION aws_conn
    );
contains:IAM authentication is not supported with password

! CREATE CONNECTION iam_requires_ssl TO MYSQL (
    HOST mysql,
    USER root,
    AWS CONNECTION aws_conn
    );
contains:IAM authentication requires SSL to be enabled
