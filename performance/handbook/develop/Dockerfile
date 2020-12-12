# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM python:3.8-slim

# Setup Python and install dependencies
ENV LANG=en_US.UTF-8
ENV PIP_DISABLE_PIP_VERSION_CHECK True
ENV PIP_NO_CACHE_DIR False
ENV PYTHONUNBUFFERED True

WORKDIR /tmp
COPY requirements.txt .
RUN pip3 install -r requirements.txt

RUN mkdir /src
WORKDIR /src
CMD ["mkdocs", "serve"]
