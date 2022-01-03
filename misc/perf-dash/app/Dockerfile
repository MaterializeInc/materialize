# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM wikirecent-server

# Override the wikirecent HTML / JS template files with our own
COPY templates /app/templates

# Override the views that we expose via TAIL
CMD ["--dbhost=perf-dash-metrics", \
     "--dbport=8675", \
     "benchmark_results", \
     "benchmark_progress", \
     "time_per_worker"]
