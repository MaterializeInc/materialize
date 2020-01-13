# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

FROM materialize/chbenchmark

RUN apt-get update && apt-get install -qy curl gnupg2

COPY mysql.asc .

RUN gpg --import mysql.asc \
    && rm mysql.asc \
    && echo "trusted-key 8C718D3B5072E1F5" >> ~/.gnupg/gpg.conf

RUN curl -fsSL https://dev.mysql.com/get/Downloads/Connector-ODBC/8.0/mysql-connector-odbc-8.0.17-linux-ubuntu18.04-x86-64bit.tar.gz > mysql-odbc.tar.gz \
    && curl -fsSL "https://dev.mysql.com/downloads/gpg/?file=mysql-connector-odbc-8.0.17-linux-ubuntu18.04-x86-64bit.tar.gz&p=10" > mysql-odbc.asc \
    && gpg --verify mysql-odbc.asc mysql-odbc.tar.gz \
    && tar -xzf mysql-odbc.tar.gz -C /usr/local --strip-components=1 --wildcards '*/bin' '*/lib' \
    && rm mysql-odbc.asc mysql-odbc.tar.gz \
    && myodbc-installer -a -d -n "MySQL ODBC 8.0" -t "Driver=/usr/local/lib/libmyodbc8a.so"

COPY odbc.ini /etc/odbc.ini

ENTRYPOINT ["chBenchmark"]
