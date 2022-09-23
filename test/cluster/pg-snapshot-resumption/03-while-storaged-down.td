# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-regex match=(\d{13}|u\d{1,3}) replacement=<REDACTED>
$ set-regex match=(\s{12}0) replacement=<ZERO>

# Increased from the default because of CI flakiness.
$ set-sql-timeout duration=10s

# The source should not yet be queryable because we fail while attempting the
# initial snapshot.
> EXPLAIN TIMESTAMP FOR SELECT COUNT(*) FROM t1
"     timestamp: <ZERO>\n         since:[<ZERO>]\n         upper:[<ZERO>]\n     has table: false\n\nsource materialize.public.mz_source (<REDACTED>, storage):\n read frontier:[<ZERO>]\nwrite frontier:[<ZERO>]\n"
