# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM ubuntu:oracular-20250619

# Ensure any Rust binaries that crash print a backtrace.
ENV RUST_BACKTRACE=1
# Disable backtrace collection in Rust libraries, such as `anyhow`. Collecting
# backtraces at runtime can be expensive and is known to cause deadlocks
# (database-issues#9159).
ENV RUST_LIB_BACKTRACE=0

ARG BUILD_PROFILE=optimize
LABEL build.profile=$BUILD_PROFILE

RUN sed -i -e 's#http://archive\.ubuntu\.com#http://us-east-1.ec2.archive.ubuntu.com#' \
           -e 's#http://security\.ubuntu\.com#http://us-east-1.ec2.archive.ubuntu.com#' \
           -e 's#http://ports\.ubuntu\.com#http://us-east-1.ec2.ports.ubuntu.com#' /etc/apt/sources.list.d/ubuntu.sources

RUN apt-get update --fix-missing && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends eatmydata
