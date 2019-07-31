# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

FROM ubuntu:bionic

RUN apt-get update && apt-get -qy install wait-for-it

COPY tests/* /tests/
COPY run-tests /usr/local/bin
COPY shlib/* /usr/local/share/shlib/

WORKDIR /workdir
