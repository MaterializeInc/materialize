# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM distroless-prod-base

COPY orchestratord /usr/local/bin/

# Antithesis symbolizes coverage from the binaries under /symbols.
ARG ANTITHESIS
USER root
RUN ["/busybox/sh", "-c", "[ -z \"$ANTITHESIS\" ] || (mkdir /symbols && ln -s /usr/local/bin/orchestratord /symbols/)"]

USER nonroot

ENTRYPOINT ["orchestratord"]
