# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# TODO: Use slim once we can pip-install
FROM python:3.9

# Install the Pure Python version of psycopg3 from source
# No effort was spent trying to optimize these steps
WORKDIR /tmp
RUN git clone https://github.com/psycopg/psycopg3.git
WORKDIR /tmp/psycopg3/psycopg3
RUN python setup.py install

COPY requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt

# Add our Python web-server source code into this container
RUN mkdir /app
COPY src /app

# Add our executable scripts into the container
COPY utils /usr/local/bin

WORKDIR /app
CMD python3 serve.py
