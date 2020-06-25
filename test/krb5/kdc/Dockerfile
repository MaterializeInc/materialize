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

RUN mkdir /var/lib/secret \
    && kdb5_util -P materialize -r CI.MATERIALIZE.IO create -s \
    && kadmin.local -q "add_principal -randkey kafka/kafka@CI.MATERIALIZE.IO" \
    && kadmin.local -q "add_principal -randkey zookeeper/zookeeper@CI.MATERIALIZE.IO" \
    && kadmin.local -q "add_principal -randkey testdrive@CI.MATERIALIZE.IO" \
    && kadmin.local -q "add_principal -randkey materialized@CI.MATERIALIZE.IO" \
    && kadmin.local -q "add_principal -randkey billing-demo@CI.MATERIALIZE.IO" \
    && kadmin.local -q "ktadd -k /var/lib/secret/kafka.key -norandkey kafka/kafka@CI.MATERIALIZE.IO " \
    && kadmin.local -q "ktadd -k /var/lib/secret/zookeeper.key -norandkey zookeeper/zookeeper@CI.MATERIALIZE.IO " \
    && kadmin.local -q "ktadd -k /var/lib/secret/testdrive.key -norandkey testdrive@CI.MATERIALIZE.IO " \
    && kadmin.local -q "ktadd -k /var/lib/secret/materialized.key -norandkey materialized@CI.MATERIALIZE.IO " \
    && kadmin.local -q "ktadd -k /var/lib/secret/billing.key -norandkey billing-demo@CI.MATERIALIZE.IO" \
    && chmod a+r /var/lib/secret/*

CMD ["krb5kdc", "-n"]
