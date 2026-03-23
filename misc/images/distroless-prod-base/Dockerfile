# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This is a separate mzimage so that we don't have to re-install the apt things
# every time we get a CI builder with a cold cache.

MZFROM ubuntu-base as builder
RUN apt-get update && apt-get install -y curl unzip
RUN curl https://awscli.amazonaws.com/awscli-exe-linux-$(arch)-2.34.15.zip > awscli.zip \
    && unzip -q awscli.zip \
    && ./aws/install \
    && rm -r awscli.zip aws

# We need python for the aws-cli, which is needed for kubernetes logins in AWS
FROM gcr.io/distroless/python3-debian13:nonroot-28078d2e5e77671d2046dcc9e2c75334e31efa4d
COPY --from=builder /usr/local/aws-cli/ /usr/local/aws-cli/
COPY --from=builder /usr/local/bin/ /usr/local/bin/
USER nonroot
ENV HOME=/home/nonroot
ENTRYPOINT []
