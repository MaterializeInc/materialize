# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Testdrive resets this value that is defaulted to true in this mzcompose, so we check
# that it is reset to the non-static default. Later on this file,
# we re-test this behavior explicitly.
> SHOW enable_envelope_upsert_in_subscribe
on

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET enable_envelope_upsert_in_subscribe

> SHOW enable_envelope_upsert_in_subscribe
on

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET ALL

> SHOW enable_envelope_upsert_in_subscribe
on
