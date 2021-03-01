# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

ARG BUILDKITE_BUILD_NUMBER

FROM materialize/materialized:v0.1.0 AS golden010
FROM materialize/materialized:v0.6.0 AS golden060
# TODO(justin); we can't use v0.7.0 because that has the bug this is testing for,
# but we should change this to v0.7.1 proper once that is released.
FROM materialize/materialized:v0.7.1-rc1 AS golden071
MZFROM materialized AS edge
MZFROM testdrive AS testdrive
FROM ubuntu:bionic-20200403

RUN apt-get update && apt-get install -y curl postgresql-client-10 wait-for-it

COPY --from=golden010 /usr/local/bin/materialized /usr/local/bin/materialized-golden010
COPY --from=golden060 /usr/local/bin/materialized /usr/local/bin/materialized-golden060
COPY --from=golden071 /usr/local/bin/materialized /usr/local/bin/materialized-golden071
COPY --from=edge /usr/local/bin/materialized /usr/local/bin/materialized-edge
COPY --from=testdrive /usr/local/bin/testdrive /usr/local/bin/testdrive
COPY catcompatck /usr/local/bin/catcompatck

ENTRYPOINT ["catcompatck"]
