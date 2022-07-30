#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This code creates TLS certificates for the various TLS-enabled services that
# appear throughout our mzcompose infrastructure. See test/kafka-ssl for a
# representative example.
#
# For each service, we generate:
#
#   * A PEM-formatted certificate and key, signed by the "ca" CA, in the files
#     SERVICE.crt and SERVICE.key.
#   * A PKCS#12-formatted archive containing the above certificate and key in a
#     file named SERVICE.p12.
#
# For Java services like Kafka, we additionally generate:
#
#   * A Java KeyStore named SERVICE.keystore.jks that contains ca.crt,
#     SERVICE.crt, and SERVICE.key.
#
#   * A PEM-formatted certificate and key, signed by the "ca-selective" CA, in
#     the files materialize-SERVICE.crt and materialize-SERVICE.key.
#
#   * A Java TrustStore named SERVICE.truststore.jks that contains ca.crt and
#     materialize-SERVICE.crt.
#
# The idea is that you configure services to use SERVICE.key as a client
# certificate when communicating with other services, and to trust ca.crt
# (or SERVICE.truststore.jks for Java services) when other services connect to it.
# The certificates signed by "ca" are then useful when you want a certificate that
# can connect to any other service.
#
# The certificates signed by "ca-selective" are useful when you want to provide
# Materialize with a certificate that can talk to one service but not another. For
# example, the materialize-kafka.key file enables communication with Kafka
# but not the Schema Registry.

set -euo pipefail

export SSL_SECRET=mzmzmz

mkdir secrets

# Create CA
openssl req \
    -x509 \
    -days 36500 \
    -newkey rsa:4096 \
    -keyout secrets/ca.key \
    -out secrets/ca.crt \
    -sha256 \
    -batch \
    -subj "/CN=MZ RSA CA" \
    -passin pass:$SSL_SECRET \
    -passout pass:$SSL_SECRET

# Create an alternative CA, used for certain tests
openssl req \
    -x509 \
    -days 36500 \
    -newkey rsa:4096 \
    -keyout secrets/ca-selective.key \
    -out secrets/ca-selective.crt \
    -sha256 \
    -batch \
    -subj "/CN=MZ RSA CA" \
    -passin pass:$SSL_SECRET \
    -passout pass:$SSL_SECRET

# create_cert CLIENT-NAME CA-NAME COMMON-NAME
create_cert() {
    local client_name=$1
    local ca_name=$2
    local common_name=$3

    # Create key & CSR.
    openssl req -nodes \
        -newkey rsa:2048 \
        -keyout secrets/"$client_name".key \
        -out tmp/"$client_name".csr \
        -sha256 \
        -batch \
        -subj "/CN=$common_name" \
        -passin pass:$SSL_SECRET \
        -passout pass:$SSL_SECRET \

    # Sign the CSR.
    openssl x509 -req \
        -CA secrets/"$ca_name".crt \
        -CAkey secrets/"$ca_name".key \
        -in tmp/"$client_name".csr \
        -out secrets/"$client_name".crt \
        -sha256 \
        -days 36500 \
        -CAcreateserial \
        -passin pass:$SSL_SECRET \

    # Export key and certificate as a PKCS#12 archive for import into JKSs.
    openssl pkcs12 \
        -export \
        -in secrets/"$client_name".crt \
        -name "$client_name" \
        -inkey secrets/"$client_name".key \
        -passin pass:$SSL_SECRET \
        -certfile secrets/"$ca_name".crt \
        -out tmp/"$client_name".p12 \
        -passout pass:$SSL_SECRET

    # Export key and certificate as a PKCS#12 archive with newer cipher
    # suites for use by OpenSSL v3+.
    openssl pkcs12 \
        -export \
        -keypbe AES-256-CBC \
        -certpbe AES-256-CBC \
        -in secrets/"$client_name".crt \
        -name "$client_name" \
        -inkey secrets/"$client_name".key \
        -passin pass:$SSL_SECRET \
        -certfile secrets/"$ca_name".crt \
        -out secrets/"$client_name".p12 \
        -passout pass:$SSL_SECRET
}

for i in materialized producer postgres certuser
do
    create_cert $i "ca" $i

done

for i in kafka kafka1 kafka2 schema-registry
do
    create_cert $i "ca" $i
    create_cert "materialized-$i" "ca-selective" "materialized"

    # Create JKS
    keytool -importkeystore \
        -deststorepass $SSL_SECRET \
        -destkeypass $SSL_SECRET \
        -srcstorepass $SSL_SECRET \
        -destkeystore secrets/$i.keystore.jks \
        -srckeystore tmp/$i.p12 \
        -srcstoretype PKCS12

    # Import CA
    keytool \
        -alias CARoot \
        -import \
        -file secrets/ca.crt \
        -keystore secrets/$i.keystore.jks \
        -noprompt -storepass $SSL_SECRET -keypass $SSL_SECRET

    # Create truststore and import CA cert
    keytool \
        -keystore secrets/$i.truststore.jks \
        -alias CARoot \
        -import \
        -file secrets/ca.crt \
        -noprompt -storepass $SSL_SECRET -keypass $SSL_SECRET

    # Create truststore and import a cert using the alternative CA
    keytool \
        -keystore secrets/$i.truststore.jks \
        -alias Selective \
        -import \
        -file secrets/materialized-$i.crt \
        -noprompt -storepass $SSL_SECRET -keypass $SSL_SECRET

done


echo $SSL_SECRET > secrets/cert_creds

# Ensure files are readable for any user.
chmod -R a+r secrets/
# The PostgreSQL key must be only user accessible to satisfy PostgreSQL's
# security checks.
cp secrets/postgres.key secrets/postgres-world-readable.key
chmod -R og-rwx secrets/postgres.key
