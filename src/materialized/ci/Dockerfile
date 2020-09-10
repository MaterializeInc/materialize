# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM ubuntu:bionic-20200403

RUN apt-get update && apt-get -qy install ca-certificates

COPY kdestroy kinit materialized /usr/local/bin/

RUN mkdir -p mzdata && chown 9000:9000 mzdata

USER 9000:9000

ENTRYPOINT ["materialized", "--log-file=stderr"]
