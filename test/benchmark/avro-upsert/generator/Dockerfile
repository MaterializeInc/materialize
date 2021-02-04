# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM kgen

FROM python:3.9-slim

COPY --from=0 /usr/local/bin/kgen /usr/local/bin

RUN mkdir -p /usr/share/generator/distributions
COPY distributions /usr/share/generator/distributions

COPY generate_upserts /usr/local/bin
ENTRYPOINT ["generate_upserts"]
