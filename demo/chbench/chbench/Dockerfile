# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM ubuntu:disco

COPY mysql.asc .
COPY odbc.ini /etc/odbc.ini
COPY . workdir/

RUN apt-get update && apt-get install -qy curl gnupg2 \
    && curl -s 'https://packages.confluent.io/deb/5.3/archive.key' | apt-key add - \
    && echo 'deb [arch=amd64] https://packages.confluent.io/deb/5.3 stable main' >> /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -qy -o Dpkg::Options::="--force-confold" \
        build-essential \
        cmake \
        confluent-schema-registry \
        curl \
        jq \
        libconfig++-dev \
        libconfig++9v5 \
        libpqxx-6.2 \
        libpqxx-dev \
        locales \
        openjdk-8-jre \
        postgresql-server-dev-11 \
        unixodbc \
        unixodbc-dev \
    && locale-gen en_US.UTF-8 \
    && gpg --import mysql.asc \
    && rm mysql.asc \
    && echo "trusted-key 8C718D3B5072E1F5" >> ~/.gnupg/gpg.conf \
    && curl -fsSL \
        https://dev.mysql.com/get/Downloads/Connector-ODBC/8.0/mysql-connector-odbc-8.0.17-linux-ubuntu18.04-x86-64bit.tar.gz \
        > mysql-odbc.tar.gz \
    && curl -fsSL \
        "https://dev.mysql.com/downloads/gpg/?file=mysql-connector-odbc-8.0.17-linux-ubuntu18.04-x86-64bit.tar.gz&p=10" \
        > mysql-odbc.asc \
    && gpg --verify mysql-odbc.asc mysql-odbc.tar.gz \
    && tar -xzf mysql-odbc.tar.gz -C /usr/local --strip-components=1 --wildcards '*/bin' '*/lib' \
    && rm mysql-odbc.asc mysql-odbc.tar.gz \
    && myodbc-installer -a -d -n "MySQL ODBC 8.0" -t "Driver=/usr/local/lib/libmyodbc8a.so" \
    && mkdir workdir/build \
    && cd workdir/build \
    && cmake -DCMAKE_BUILD_TYPE=Release .. \
    && make \
    && mv chbenchmark /usr/local/bin/chBenchmark \
    && cd .. \
    && rm -rf build \
    && apt-get remove -y \
        build-essential \
        cmake \
        confluent-schema-registry \
        curl \
        jq \
        libconfig++-dev \
        libpqxx-dev \
        locales \
        openjdk-8-jre \
        postgresql-server-dev-11 \
        unixodbc-dev \
    && apt-get autoremove -y \
    ;

COPY ./flush-tables /usr/local/bin/flush-tables
COPY ./mz-default.cfg /etc/chbenchmark/mz-default.cfg

ENTRYPOINT ["chBenchmark"]
