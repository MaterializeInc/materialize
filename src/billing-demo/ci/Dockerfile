# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM ubuntu-base

COPY billing-demo /usr/local/bin

RUN mkdir -p /share/billing-demo/data && chmod 777 /share/billing-demo/data
VOLUME /share/billing-demo/data

ENTRYPOINT ["billing-demo"]
