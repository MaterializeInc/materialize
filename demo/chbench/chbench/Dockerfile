# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM ubuntu-base

COPY mysql.asc .
COPY odbc.ini /etc/odbc.ini
COPY . workdir/

ARG DEBIAN_FRONTEND="noninteractive"

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
        libmysqlclient-dev \
        libpqxx-6.4 \
        libpqxx-dev \
        locales \
        odbc-postgresql \
        openjdk-8-jre \
        postgresql-server-dev-12 \
        unixodbc \
        unixodbc-dev \
    && locale-gen en_US.UTF-8 \
    && gpg --import mysql.asc \
    && rm mysql.asc \
    && echo "trusted-key 8C718D3B5072E1F5" >> ~/.gnupg/gpg.conf \
    && curl -fsSL \
        https://dev.mysql.com/get/Downloads/Connector-ODBC/8.0/mysql-connector-odbc-8.0.27-src.tar.gz \
        > mysql-odbc.tar.gz \
    && curl -fsSL \
        "https://dev.mysql.com/downloads/gpg/?file=mysql-connector-odbc-8.0.27-src.tar.gz&p=10" \
        > mysql-odbc.asc \
    && tar -xzf mysql-odbc.tar.gz \
    && rm mysql-odbc.asc mysql-odbc.tar.gz \
    && (cd mysql-connector-odbc-8.0.27-src && cmake -DWITH_UNIXODBC=1 . && make -j8 myodbc-installer && make install) \
    && rm -r mysql-connector-odbc-8.0.27-src \
    && myodbc-installer -a -d -n "Mysql" -t "Driver=/usr/local/lib/libmyodbc8a.so" \
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
        postgresql-server-dev-12 \
    && apt-get autoremove -y \
    ;

COPY ./mz-default-postgres.cfg /etc/chbenchmark/mz-default-postgres.cfg
COPY ./mz-default-mysql.cfg /etc/chbenchmark/mz-default-mysql.cfg

ENTRYPOINT ["chBenchmark"]
