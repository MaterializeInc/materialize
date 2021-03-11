# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

ARG BUILDKITE_BUILD_NUMBER

MZFROM materialized AS edge
MZFROM testdrive AS testdrive
FROM ubuntu:bionic-20200403

RUN apt-get update && apt-get install -y curl postgresql-client-10 wait-for-it

COPY --from=edge /usr/local/bin/materialized /usr/local/bin/materialized-edge
COPY --from=testdrive /usr/local/bin/testdrive /usr/local/bin/testdrive
COPY exactlyonceck /usr/local/bin/exactlyonceck

ENTRYPOINT ["exactlyonceck"]
