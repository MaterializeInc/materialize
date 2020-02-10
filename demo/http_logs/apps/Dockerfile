# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM ubuntu:bionic

RUN apt-get update -o Acquire::Languages=none \
    && apt-get install -qy -o Dpkg::Options::=--force-unsafe-io \
         aspell \
    && aspell -d en dump master | aspell -l en expand | grep '^[a-z]*$' > words.txt \
    && apt-get remove -y aspell \
    && apt-get autoremove -y

FROM python:3.8-slim

COPY . /app
COPY --from=0 words.txt /app/words.txt
WORKDIR /app

RUN pip install -r requirements-server.txt -r requirements-loadgen.txt

CMD ["./server.py"]
