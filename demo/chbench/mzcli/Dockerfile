# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

FROM ubuntu:bionic

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

COPY mzcli-mtrlz.zip /tmp

RUN apt-get update \
    && apt-get upgrade -qy \
    && apt-get install -qy less postgresql-client \
    && apt-get install -qy --no-install-recommends \
        build-essential \
        libpq-dev \
        python3-minimal \
        python3-dev \
        python3-pip \
        python3-setuptools \
        python3-wheel \
        python3-setuptools \
    && pip3 install psycopg2-binary \
    && pip3 install /tmp/mzcli-mtrlz.zip \
    && apt-get remove --purge -qy \
        build-essential \
        libpq-dev \
    && apt-get autoremove -qy \
    ;

COPY watch-sql /usr/local/bin/watch-sql

CMD ["mzcli", "host=materialized port=6875 sslmode=disable database=materialize"]
