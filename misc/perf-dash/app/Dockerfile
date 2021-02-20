# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# TODO: Use slim once we can pip-install without requiring git
FROM python:3.9

WORKDIR /tmp
COPY requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt

# Add our Python web-server source code into this container
RUN mkdir /app
COPY src /app

# Add our executable scripts into the container
COPY utils /usr/local/bin

WORKDIR /app
ENTRYPOINT ["python3", "serve.py"]
