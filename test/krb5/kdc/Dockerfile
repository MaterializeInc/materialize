# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM ubuntu:bionic-20200403

RUN apt-get update && apt-get install -y \
    krb5-admin-server \
    krb5-kdc \
    krb5-user

COPY krb5.conf /etc/krb5.conf

RUN mkdir -p /var/log/kerberos /etc/kdc /var/kerberos/krb5kdc /var/lib/secret \
    && ln -s /etc/krb5.conf /etc/kdc/krb5.conf \
    && kdb5_util -P confluent -r CI.MATERIALIZE.IO create -s \
    && kadmin.local -w password -q "add_principal -randkey kafka/broker@CI.MATERIALIZE.IO" \
    && kadmin.local -w password -q "add_principal -randkey zookeeper/zookeeper@CI.MATERIALIZE.IO" \
    && kadmin.local -w password -q "add_principal -randkey testdrive@CI.MATERIALIZE.IO" \
    && kadmin.local -w password -q "add_principal -randkey materialized@CI.MATERIALIZE.IO" \
    && kadmin.local -w password -q "ktadd -k /var/lib/secret/broker.key -norandkey kafka/broker@CI.MATERIALIZE.IO " \
    && kadmin.local -w password -q "ktadd -k /var/lib/secret/zookeeper.key -norandkey zookeeper/zookeeper@CI.MATERIALIZE.IO " \
    && kadmin.local -w password -q "ktadd -k /var/lib/secret/testdrive.key -norandkey testdrive@CI.MATERIALIZE.IO " \
    && kadmin.local -w password -q "ktadd -k /var/lib/secret/materialized.key -norandkey materialized@CI.MATERIALIZE.IO " \
    && chmod a+r /var/lib/secret/*

CMD ["krb5kdc", "-n"]
