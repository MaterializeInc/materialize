# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM python:3.9-slim

RUN pip install psycopg2-binary

# This is the directory that we expect to be mounted by Docker so that we can
# save files across Docker runs
RUN mkdir -p /snapshot

COPY await_view_states.py /usr/local/bin/await_view_states.py
RUN chmod +x /usr/local/bin/await_view_states.py

COPY snapshot_view_states.py /usr/local/bin/snapshot_view_states.py
RUN chmod +x /usr/local/bin/snapshot_view_states.py
