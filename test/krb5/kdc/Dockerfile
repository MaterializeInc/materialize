# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Portions of this file are derived from
# https://github.com/vdesabou/kafka-docker-playground/blob/master/environment/kerberos/kdc/Dockerfile

FROM ubuntu:bionic

RUN apt-get update \
    && apt-get upgrade -qy \
    && apt-get install krb5-kdc krb5-admin-server -qy \
    && apt-get install krb5-user -qy

COPY krb5.conf /etc/krb5.conf
RUN mkdir /var/log/kerberos
RUN mkdir /etc/kdc
RUN mkdir -p /var/kerberos/krb5kdc/
RUN ln -s /etc/krb5.conf /etc/kdc/krb5.conf

EXPOSE 88

RUN kdb5_util -P confluent -r CI.MATERIALIZE.IO create -s

COPY init.sh .
RUN chmod o+x ./init.sh
RUN ./init.sh

CMD /usr/sbin/krb5kdc -n
