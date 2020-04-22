#!/bin/sh

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This file is derived from
# https://github.com/Pierrotws/kafka-ssl-compose/blob/master/create-certs.sh

export SSL_SECRET=mzmzmz

mkdir secrets

openssl req \
	-x509 \
	-days 3650 \
	-newkey rsa:4096 \
	-keyout secrets/ca.key \
	-out secrets/ca.crt \
	-sha256 \
	-batch \
	-subj "/CN=MZ RSA CA" \
	-passin pass:"$SSL_SECRET" \
	-passout pass:"$SSL_SECRET"

for i in 'broker' 'schema-registry' 'materialized' 'producer'
do
	# Create keystores
	keytool -genkey \
		-noprompt \
		-alias $i \
		-dname "CN=$i" \
		-keystore secrets/$i.keystore.jks \
		-keyalg RSA \
		-sigalg SHA256withRSA \
		-keysize 2048 \
		-storepass "$SSL_SECRET" \
		-keypass "$SSL_SECRET"

	# Create CSR
	keytool -keystore secrets/$i.keystore.jks \
		-alias $i \
		-certreq \
		-file secrets/$i.csr \
		-storepass "$SSL_SECRET" -keypass "$SSL_SECRET" \
		-ext SAN=dns:$i

	# Sign the CSR; note that the extensions are necessary to fulfill the
	# following requirements of rustls/webpki:
	# - Add SAN to cert
	# - Create a v3 cert
	openssl x509 -req \
		-CA secrets/ca.crt \
		-CAkey secrets/ca.key \
		-in secrets/$i.csr \
		-out secrets/$i.crt \
		-sha256 \
		-days 3650 \
		-CAcreateserial \
		-passin pass:"$SSL_SECRET" \
		-extensions $i -extfile openssl.cnf

	# Import the CA
	keytool \
		-alias caroot \
		-import \
		-file secrets/ca.crt \
		-keystore secrets/$i.keystore.jks \
		-noprompt -storepass "$SSL_SECRET" -keypass "$SSL_SECRET"

	# Import the cert
	keytool \
		-keystore secrets/$i.keystore.jks \
		-alias $i \
		-import \
		-file secrets/$i.crt \
		-noprompt -storepass "$SSL_SECRET" -keypass "$SSL_SECRET"

	# Create truststore and import the CA cert.
	keytool \
		-keystore secrets/$i.truststore.jks \
		-alias caroot \
		-import \
		-file secrets/ca.crt \
		-noprompt -storepass "$SSL_SECRET" -keypass "$SSL_SECRET"
done

# Export cert, key, and ca root cert for use in connecting to librdkafka.
keytool -exportcert \
	-alias materialized \
	-keystore secrets/materialized.keystore.jks \
	-noprompt -storepass "$SSL_SECRET" -keypass "$SSL_SECRET" \
	-rfc \
	-file secrets/certificate.pem

keytool -v -importkeystore \
	-srckeystore secrets/materialized.keystore.jks \
	-srcalias materialized \
	-destkeystore secrets/materialized-cert_and_key.p12 \
	-deststoretype PKCS12 \
	-noprompt -storepass "$SSL_SECRET" -keypass "$SSL_SECRET" \
	-srcstorepass "$SSL_SECRET" -srckeypass "$SSL_SECRET"

openssl pkcs12 \
	-in secrets/materialized-cert_and_key.p12 \
	-nocerts -nodes \
	-password pass:"$SSL_SECRET" > secrets/key.pem

keytool -exportcert \
	-alias caroot \
	-keystore secrets/materialized.keystore.jks \
	-noprompt -storepass "$SSL_SECRET" -keypass "$SSL_SECRET" \
	-rfc \
	-file secrets/ca_root.pem

echo "$SSL_SECRET" > secrets/cert_creds

rm -rf tmp
