# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

FROM ubuntu:bionic

RUN apt-get update && apt-get -qy install libssl1.1 wait-for-it

COPY sqllogictest /usr/local/bin

WORKDIR /workdir
