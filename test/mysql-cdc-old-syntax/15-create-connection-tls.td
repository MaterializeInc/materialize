# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'


#
# Validate MySQL server CA and client TLS/SSL client options
#

> CREATE SECRET ssl_ca AS '${arg.ssl-ca}'
> CREATE SECRET ssl_client_cert AS '${arg.ssl-client-cert}'
> CREATE SECRET ssl_client_key AS '${arg.ssl-client-key}'
> CREATE SECRET ssl_wrong_ca AS '${arg.ssl-wrong-ca}'
> CREATE SECRET ssl_wrong_client_cert AS '${arg.ssl-wrong-client-cert}'
> CREATE SECRET ssl_wrong_client_key AS '${arg.ssl-wrong-client-key}'

> CREATE SECRET mysqluserpass AS '${arg.mysql-user-password}'

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
CREATE USER 'norm_user' IDENTIFIED BY '${arg.mysql-user-password}';
CREATE USER 'tls_user' IDENTIFIED BY '${arg.mysql-user-password}' REQUIRE SSL;
CREATE USER 'tls_cert_user' IDENTIFIED BY '${arg.mysql-user-password}' REQUIRE X509;

# Success: Disabled SSL Mode with normal user
> CREATE CONNECTION mysq_tls TO MYSQL (
    HOST mysql,
    USER norm_user,
    PASSWORD SECRET mysqluserpass,
    SSL MODE disabled
  )
> DROP CONNECTION mysq_tls;

# Error: Disabled SSL Mode with required TLS user
! CREATE CONNECTION mysq_tls TO MYSQL (
    HOST mysql,
    USER tls_user,
    PASSWORD SECRET mysqluserpass,
    SSL MODE disabled
  )
contains:Access denied for user

# Success: Required SSL mode with required TLS user
> CREATE CONNECTION mysq_tls TO MYSQL (
    HOST mysql,
    USER tls_user,
    PASSWORD SECRET mysqluserpass,
    SSL MODE required
  )
> DROP CONNECTION mysq_tls;

# Error: Required SSL mode with required x509 Cert user
! CREATE CONNECTION mysq_tls TO MYSQL (
    HOST mysql,
    USER tls_cert_user,
    PASSWORD SECRET mysqluserpass,
    SSL MODE required
  )
contains:Access denied for user

# Success: Required SSL mode + client cert with required x509 Cert user
> CREATE CONNECTION mysq_tls TO MYSQL (
    HOST mysql,
    USER tls_cert_user,
    PASSWORD SECRET mysqluserpass,
    SSL MODE required,
    SSL CERTIFICATE SECRET ssl_client_cert,
    SSL KEY SECRET ssl_client_key
  )
> DROP CONNECTION mysq_tls;

# Error: Required SSL mode + wrong client cert with required x509 Cert user
! CREATE CONNECTION mysq_tls TO MYSQL (
    HOST mysql,
    USER tls_cert_user,
    PASSWORD SECRET mysqluserpass,
    SSL MODE required,
    SSL CERTIFICATE SECRET ssl_wrong_client_cert,
    SSL KEY SECRET ssl_wrong_client_key
  )
contains:Input/output error

# Success: Verify_CA SSL mode
> CREATE CONNECTION mysq_tls TO MYSQL (
    HOST mysql,
    USER tls_user,
    PASSWORD SECRET mysqluserpass,
    SSL MODE verify_ca,
    SSL CERTIFICATE AUTHORITY SECRET ssl_ca
  )
> DROP CONNECTION mysq_tls;

# Error: Verify_CA SSL mode without providing Server CA
! CREATE CONNECTION mysq_tls TO MYSQL (
    HOST mysql,
    USER tls_user,
    PASSWORD SECRET mysqluserpass,
    SSL MODE verify_ca
  )
contains:TLS error

# Error: Verify_CA SSL mode with wrong Server CA
! CREATE CONNECTION mysq_tls TO MYSQL (
    HOST mysql,
    USER tls_user,
    PASSWORD SECRET mysqluserpass,
    SSL MODE verify_ca,
    SSL CERTIFICATE AUTHORITY SECRET ssl_wrong_ca
  )
contains:TLS error

# Error: Verify_CA SSL mode with required x509 Cert user and no client cert
! CREATE CONNECTION mysq_tls TO MYSQL (
    HOST mysql,
    USER tls_cert_user,
    PASSWORD SECRET mysqluserpass,
    SSL MODE verify_ca,
    SSL CERTIFICATE AUTHORITY SECRET ssl_ca
  )
contains:Access denied for user

# Success: Verify_CA SSL mode with required x509 Cert user
> CREATE CONNECTION mysq_tls TO MYSQL (
    HOST mysql,
    USER tls_cert_user,
    PASSWORD SECRET mysqluserpass,
    SSL MODE verify_ca,
    SSL CERTIFICATE AUTHORITY SECRET ssl_ca,
    SSL CERTIFICATE SECRET ssl_client_cert,
    SSL KEY SECRET ssl_client_key
  )
> DROP CONNECTION mysq_tls;

# Success: Verify_CA SSL mode with required x509 Cert user and wrong client cert
! CREATE CONNECTION mysq_tls TO MYSQL (
    HOST mysql,
    USER tls_cert_user,
    PASSWORD SECRET mysqluserpass,
    SSL MODE verify_ca,
    SSL CERTIFICATE AUTHORITY SECRET ssl_ca,
    SSL CERTIFICATE SECRET ssl_wrong_client_cert,
    SSL KEY SECRET ssl_wrong_client_key
  )
contains: Input/output error

# TODO: Figure out how to test the Verify_Identity SSL Mode with the auto-generated certs
# created by MySQL. They use an odd CN value in the CA cert:
# https://dev.mysql.com/doc/refman/8.3/en/creating-ssl-rsa-files-using-mysql.html#creating-ssl-rsa-files-using-mysql-ssl-and-rsa-file-characteristics
