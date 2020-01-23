# Copyright 2020 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

FROM python:3.8

COPY . /app
WORKDIR /app

RUN pip install -r requirements.txt

CMD ["./app.py"]
