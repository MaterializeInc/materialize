# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Build and run SQLsmith ported for Materialize

MZFROM ubuntu-base

WORKDIR /workdir

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    python3 \
    python3-confluent-kafka \
    python3-requests \
    python3-pip && pip3 install fastavro


COPY . data-ingest/

#ENTRYPOINT ["python3", "data-ingest/data-ingest.py"]
ENTRYPOINT ["bash"]
