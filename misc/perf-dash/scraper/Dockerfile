# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM ubuntu-base

ARG DEBIAN_FRONTEND="noninteractive"

RUN apt-get update && apt-get install -y librdkafka-dev python3-dev python3 python3-pip tzdata && apt-get clean

COPY requirements.txt /tmp
RUN pip3 install -r /tmp/requirements.txt

RUN mkdir -p /usr/share/perf-scraper/metrics
COPY metrics /usr/share/perf-scraper/metrics

COPY scrape_system_metrics /usr/local/bin
ENTRYPOINT ["scrape_system_metrics"]
CMD ["scrape"]
