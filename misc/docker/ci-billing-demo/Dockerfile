# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

FROM ubuntu:bionic

RUN apt-get update && apt-get install -qy postgresql-client
RUN apt-get install -qy wait-for-it

COPY billing-demo /usr/local/bin

ENTRYPOINT ["billing-demo"]
