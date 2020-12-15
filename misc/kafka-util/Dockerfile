# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# When this was written, Python3.7 was the most recent version that pyarrow supported
FROM python:3.7-slim

# This is the directory that we expect to be mounted by Docker so that we can
# save files across Docker runs
RUN mkdir -p /snapshot

COPY requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt

# Add our executable scripts into the container
COPY scripts /usr/local/bin

WORKDIR /snapshot
