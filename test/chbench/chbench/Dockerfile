# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM debian-base AS build

ARG ARCH_GCC

RUN apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y \
        build-essential \
        cmake \
        curl \
        gnupg2 \
        libconfig++-dev \
        libpqxx-dev \
        unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install the MySQL ODBC connector.

COPY mysql.asc .

RUN arch_mysql=$(echo "$ARCH_GCC" | sed -e "s/x86_64/x86-64bit/") \
    && archive="mysql-connector-odbc-9.6.0-linux-glibc2.28-$arch_mysql.tar.gz" \
    && directory=${archive%.tar.gz} \
    && gpg --import mysql.asc \
    && echo "trusted-key B7B3B788A8D3785C" >> ~/.gnupg/gpg.conf \
    && curl -fsSL "https://dev.mysql.com/get/Downloads/Connector-ODBC/9.6/$archive" > mysql-odbc.tar.gz \
    && curl -fsSL "https://dev.mysql.com/downloads/gpg/?file=$archive&p=10" > mysql-odbc.asc \
    && gpg --verify mysql-odbc.asc mysql-odbc.tar.gz \
    && mkdir /mysql-connector-odbc \
    && tar -xzf mysql-odbc.tar.gz --strip-components=2 -C /mysql-connector-odbc "$directory/lib"

# Build chBenchmark.

COPY . src/

RUN mkdir build && cd build \
    && cmake -DCMAKE_BUILD_TYPE=Release ../src \
    && make -j8

RUN ls build

# Copy the build artifacts into a fresh image.

MZFROM debian-base

RUN apt-get update && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y libconfig++11 libpqxx-7.10 unixodbc && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY odbc.ini /etc/odbc.ini
COPY mz-default-postgres.cfg /etc/chbenchmark/mz-default-postgres.cfg
COPY mz-default-mysql.cfg /etc/chbenchmark/mz-default-mysql.cfg
COPY --from=build /mysql-connector-odbc /usr/local/lib/mysql-connector-odbc
COPY --from=build /build/chbenchmark /usr/local/bin/chBenchmark

ENTRYPOINT ["chBenchmark"]
