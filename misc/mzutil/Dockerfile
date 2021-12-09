# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM python:3.9-slim

RUN apt-get update && apt-get install -y build-essential libpq-dev python-dev

COPY requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt

# This is the directory that we expect to be mounted by Docker so that we can
# save files across Docker runs
RUN mkdir -p /snapshot

# Add our executable scripts into the container
COPY scripts /usr/local/bin
