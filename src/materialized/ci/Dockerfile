# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM materialized-base

COPY listener_configs/ /listener_configs/
COPY materialized entrypoint.sh /usr/local/bin/

USER root
RUN ln -s /usr/local/bin/materialized /usr/local/bin/environmentd \
  && ln -s /usr/local/bin/materialized /usr/local/bin/clusterd

USER materialize

RUN bash -c '\
  set -euo pipefail; \
  tini -- entrypoint.sh & \
  tini_pid=$!; \
  start_time=$(date +%s); \
  while ! curl -sf localhost:6878/api/readyz; do \
    sleep 0.1; \
    now=$(date +%s); \
    if (( now - start_time > 30 )); then \
      echo "Timed out, failing build"; \
      exit 1; \
    fi; \
  done; \
  kill -TERM "$tini_pid"; \
  wait "$tini_pid" || true; \
  pg_ctlcluster 16 main stop --mode=fast --force'

ENTRYPOINT ["tini", "--", "entrypoint.sh"]
