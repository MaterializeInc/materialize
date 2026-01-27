# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM foundationdb/foundationdb:7.3.71

COPY setup-foundationdb.sh /usr/local/bin/setup-foundationdb.sh
RUN chmod +x /usr/local/bin/setup-foundationdb.sh

ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/usr/local/bin/setup-foundationdb.sh"]

HEALTHCHECK --interval=2s --timeout=5s --start-period=10s --retries=3 \
    CMD fdbcli -C "${FDB_CLUSTER_FILE:-/var/fdb/fdb.cluster}" --exec "status minimal" --timeout 3 || exit 1
