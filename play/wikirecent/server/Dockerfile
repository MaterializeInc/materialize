# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM ubuntu-base

RUN apt-get update && apt-get install -y libpq5 python3 python3-pip

COPY requirements.txt .
RUN pip install -r requirements.txt && rm requirements.txt

WORKDIR /server
COPY index.html serve.py ./
ENTRYPOINT ["python3", "serve.py"]
