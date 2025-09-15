# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM materialized-base

COPY listener_configs/ /listener_configs/
COPY materialized entrypoint.sh /usr/local/bin/

USER root
RUN ln -s /usr/local/bin/materialized /usr/local/bin/environmentd \
  && ln -s /usr/local/bin/materialized /usr/local/bin/clusterd

USER materialize

RUN /usr/lib/postgresql/16/bin/initdb -D /mzdata/postgres -U root --auth-local=trust

ENTRYPOINT ["tini", "--", "entrypoint.sh"]
