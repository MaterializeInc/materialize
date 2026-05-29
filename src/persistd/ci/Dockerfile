# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM distroless-prod-base

# Workspace-wide cargo feature unification compiles `mz-persist` with the
# `foundationdb` feature (enabled by `materialized`), so the `persistd` binary
# carries a `DT_NEEDED libfdb_c.so` entry even though it does not itself enable
# the feature. Bundle the client library so the binary loads, mirroring
# `ubuntu-base` which does the same for `materialized`.
COPY --from=foundationdb/foundationdb:7.3.71 /usr/lib/libfdb_c.so /usr/lib/

COPY persistd /usr/local/bin/persistd

ENTRYPOINT ["persistd"]
