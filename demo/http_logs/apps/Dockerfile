# Copyright 2020 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

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
