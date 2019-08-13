# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

FROM ubuntu:bionic

RUN apt-get update && apt-get install -qy less postgresql-client

COPY watch-sql /usr/local/bin/watch-sql

CMD ["psql", "-h", "materialized", "-p", "6875", "sslmode=disable"]
