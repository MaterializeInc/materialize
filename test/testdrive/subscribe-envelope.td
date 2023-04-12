# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-regex match=\d{13,20} replacement=<TIMESTAMP>

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_envelope_upsert_in_subscribe = true

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT 1 as a, 2 as b union select 2, 1) ENVELOPE UPSERT KEY (a, b)
> FETCH 1 c
<TIMESTAMP> 1 1 2

> FETCH 1 c
<TIMESTAMP> 1 2 1
> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT 1 as a, 2 as b union select 2, 1) ENVELOPE UPSERT KEY (b, a)
> FETCH 1 c
<TIMESTAMP> 1 2 1

> FETCH 1 c
<TIMESTAMP> 1 1 2
> COMMIT

! SUBSCRIBE (SELECT 1 as a, 2 as b union select 2, 1) ENVELOPE UPSERT KEY (c)
contains: No such column: c

! SUBSCRIBE (SELECT 1 as c, 2 as c union select 2, 1) ENVELOPE UPSERT KEY (c)
contains: Ambiguous column: c
