# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

ARG BUILDKITE_BUILD_NUMBER

FROM materialize/materialized:v0.1.0 AS golden
FROM materialize/ci-materialized:$BUILDKITE_BUILD_NUMBER AS edge
FROM materialize/ci-testdrive:$BUILDKITE_BUILD_NUMBER AS testdrive
FROM ubuntu:bionic

RUN apt-get update && apt-get install -y curl postgresql-client-10 wait-for-it

COPY --from=golden /usr/local/bin/materialized /usr/local/bin/materialized-golden
COPY --from=edge /usr/local/bin/materialized /usr/local/bin/materialized-edge
COPY --from=testdrive /usr/local/bin/testdrive /usr/local/bin/testdrive
COPY catcompatck /usr/local/bin/catcompatck

ENTRYPOINT ["catcompatck"]
