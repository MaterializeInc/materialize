#!/bin/bash

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Portions of this file are derived from
# https://github.com/vdesabou/kafka-docker-playground/blob/master/environment/kerberos/start.sh

set -e

mkdir /var/lib/secret

# Create identities:
# - Kafka service principal
kadmin.local -w password -q "add_principal -randkey kafka/broker.krb5.local@CI.MATERIALIZE.IO"
# - Zookeeper service principal
kadmin.local -w password -q "add_principal -randkey zookeeper/zookeeper.krb5.local@CI.MATERIALIZE.IO"
# - Client principals
kadmin.local -w password -q "add_principal -randkey testdrive@CI.MATERIALIZE.IO"
kadmin.local -w password -q "add_principal -randkey materialized@CI.MATERIALIZE.IO"

# Create keytabs:
kadmin.local -w password -q "ktadd -k /var/lib/secret/broker.key -norandkey kafka/broker.krb5.local@CI.MATERIALIZE.IO "
kadmin.local -w password -q "ktadd -k /var/lib/secret/zookeeper.key -norandkey zookeeper/zookeeper.krb5.local@CI.MATERIALIZE.IO "
kadmin.local -w password -q "ktadd -k /var/lib/secret/testdrive.key -norandkey testdrive@CI.MATERIALIZE.IO "
kadmin.local -w password -q "ktadd -k /var/lib/secret/materialized.key -norandkey materialized@CI.MATERIALIZE.IO "

# Ensure keytabs are readable
chmod a+r /var/lib/secret/broker.key
chmod a+r /var/lib/secret/zookeeper.key
chmod a+r /var/lib/secret/testdrive.key
chmod a+r /var/lib/secret/materialized.key
