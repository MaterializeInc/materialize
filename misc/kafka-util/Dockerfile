# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM python:3.7-slim

RUN apt update && apt install -y librdkafka1 librdkafka-dev

ENV RDKAFKA_INSTALL=system
RUN pip install pykafka \
                pyarrow \
                requests

# This is the directory that we expect to be mounted by Docker so that we can
# save files across Docker runs
RUN mkdir -p /snapshot

COPY archive_topics.py /usr/local/bin/archive_topics.py
RUN chmod +x /usr/local/bin/archive_topics.py

COPY restore_topics.py /usr/local/bin/restore_topics.py
RUN chmod +x /usr/local/bin/restore_topics.py

COPY summarize_topics.py /usr/local/bin/summarize_topics.py
RUN chmod +x /usr/local/bin/summarize_topics.py

WORKDIR /snapshot
