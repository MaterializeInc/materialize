# Copyright Materialize, Inc. and contributors. All rights reserved.
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.

FROM postgres:18.4
RUN apt-get update && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/*
COPY run.py /usr/local/bin/qps-pgbench.py
ENTRYPOINT ["python3", "/usr/local/bin/qps-pgbench.py"]
