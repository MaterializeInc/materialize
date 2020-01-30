# Copyright Copyright 2019-2020 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

FROM ubuntu:bionic

RUN apt-get update && apt-get install -qy aspell

RUN aspell -d en dump master | aspell -l en expand | grep '^[a-z]*$' > words.txt

FROM python:3.8

COPY . /app
COPY --from=0 words.txt /app/words.txt
WORKDIR /app

RUN pip install -r requirements.txt

CMD ["./main.py"]
