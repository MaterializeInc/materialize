# Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

FROM ubuntu:bionic

RUN apt-get update && apt-get -qy install wait-for-it

COPY testdrive /usr/local/bin

WORKDIR /workdir

RUN mkdir -p /tmpshare && chmod 777 /tmpshare

VOLUME ["/tmpshare"]
