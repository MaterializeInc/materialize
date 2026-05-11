# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Antithesis config image: a FROM-scratch tarball holding the resolved
# docker-compose.yaml that Antithesis uses to bring up the system under
# test. See mzbuild.yml for regeneration instructions.

FROM scratch
COPY docker-compose.yaml /
