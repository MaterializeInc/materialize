# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM test-certs AS certs

FROM mysql:9.5.0 AS seed

ENV MYSQL_ROOT_HOST=%

COPY seed-mysql-datadir.sh /usr/local/bin/seed-mysql-datadir.sh

RUN chmod +x /usr/local/bin/seed-mysql-datadir.sh \
    && mkdir -p /var/lib/mysql-seed /var/lib/mysql-files \
    && chown -R mysql:mysql /var/lib/mysql-seed /var/lib/mysql-files \
    && gosu mysql /usr/local/bin/seed-mysql-datadir.sh

FROM mysql:9.5.0

COPY --from=seed /var/lib/mysql-seed /var/lib/mysql-seed
COPY mysql-entrypoint.sh /usr/local/bin/mysql-entrypoint.sh

RUN chmod +x /usr/local/bin/mysql-entrypoint.sh

ENTRYPOINT ["mysql-entrypoint.sh"]
