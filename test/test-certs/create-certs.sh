#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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

for i in kafka kafka1 kafka2 schema-registry materialized producer postgres certuser
do
	# Create key & csr
	openssl req -nodes \
		-newkey rsa:2048 \
		-keyout secrets/$i.key \
		-out tmp/$i.csr \
		-sha256 \
		-batch \
		-subj "/CN=$i" \
		-passin pass:$SSL_SECRET \
		-passout pass:$SSL_SECRET \

	# Sign the CSR.
	openssl x509 -req \
		-CA secrets/ca.crt \
		-CAkey secrets/ca.key \
		-in tmp/$i.csr \
		-out secrets/$i.crt \
		-sha256 \
		-days 36500 \
		-CAcreateserial \
		-passin pass:$SSL_SECRET \

	# Export key and certificate as a PKCS#12 archive for import into JKSs.
	openssl pkcs12 \
		-export \
		-in secrets/$i.crt \
		-name $i \
		-inkey secrets/$i.key \
		-passin pass:$SSL_SECRET \
		-certfile secrets/ca.crt \
		-out tmp/$i.p12 \
		-passout pass:$SSL_SECRET

	# Export key and certificate as a PKCS#12 archive with newer cipher
    # suites for use by OpenSSL v3+.
	openssl pkcs12 \
		-export \
        -keypbe AES-256-CBC \
        -certpbe AES-256-CBC \
		-in secrets/$i.crt \
		-name $i \
		-inkey secrets/$i.key \
		-passin pass:$SSL_SECRET \
		-certfile secrets/ca.crt \
		-out secrets/$i.p12 \
		-passout pass:$SSL_SECRET

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

done

echo $SSL_SECRET > secrets/cert_creds

# Ensure files are readable for any user
chmod -R a+r secrets/
# Keys are only user-accessible
chmod -R og-rwx secrets/*.key
