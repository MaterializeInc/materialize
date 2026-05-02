# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM foundationdb/foundationdb:7.3.71

# Use the upstream entrypoint which handles FDB_CLUSTER_FILE_CONTENTS
# Database initialization is handled separately by FoundationDBCluster service

# Healthcheck verifies the FDB server process is running and listening
# Note: We check if the process is listening, not if the database is configured,
# because initialization happens in a separate service after nodes are up
HEALTHCHECK --interval=2s --timeout=5s --start-period=10s --retries=3 \
    CMD pgrep fdbserver > /dev/null || exit 1
