# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test creating a MySQL source using SSH and SSL options
# More comprehensive SSL option tests are in
# `test/mysql-cdc/15-create-connection-tls.td`

> CREATE SECRET ssl_ca AS '${arg.ssl-ca}'
> CREATE SECRET ssl_client_cert AS '${arg.ssl-client-cert}'
> CREATE SECRET ssl_client_key AS '${arg.ssl-client-key}'

# Basic TLS
> CREATE CONNECTION mysql_ssl TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass,
    SSH TUNNEL thancred,
    SSL MODE required
  );
> DROP CONNECTION mysql_ssl;

# TLS with CA verification and a client cert
> CREATE CONNECTION mysql_ssl TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass,
    SSL MODE verify_ca,
    SSL CERTIFICATE AUTHORITY SECRET ssl_ca,
    SSL CERTIFICATE SECRET ssl_client_cert,
    SSL KEY SECRET ssl_client_key
  );

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS tls_schema;
CREATE DATABASE tls_schema;
USE tls_schema;
CREATE TABLE tls_data (f1 INTEGER);
INSERT INTO tls_data VALUES (1), (2);
COMMIT;

> BEGIN
> CREATE SOURCE mysql_source_ssl FROM MYSQL
  CONNECTION mysql_ssl;

> CREATE TABLE tls_data FROM SOURCE mysql_source_ssl (REFERENCE tls_schema.tls_data);
> COMMIT

> SELECT COUNT(*) FROM tls_data;
2

# TODO: Figure out how to test the Verify_Identity SSL Mode with the auto-generated certs
# created by MySQL. They use an odd CN value in the CA cert:
# https://dev.mysql.com/doc/refman/8.3/en/creating-ssl-rsa-files-using-mysql.html#creating-ssl-rsa-files-using-mysql-ssl-and-rsa-file-characteristics
