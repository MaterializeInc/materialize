# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Use `prod-base` (Ubuntu) rather than `distroless-prod-base`. Workspace-wide
# cargo feature unification compiles `mz-persist` with the same heavy
# dependency set as `environmentd` (FoundationDB, plus xz/lzma via the
# arrow/parquet stack), so the `persistd` binary needs the same runtime shared
# libraries (`libfdb_c.so`, `liblzma.so.5`, ...). `prod-base` provides them via
# `ubuntu-base`; the distroless image does not, and bundling each library by
# hand is whack-a-mole. This matches `environmentd` and `clusterd`.
MZFROM prod-base

COPY persistd /usr/local/bin/persistd

USER materialize

ENTRYPOINT ["tini", "--", "persistd"]
