# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test fails different depending on where it gets run from.
#
# psql cli will see this failure
#  COPY FROM: error: reqwest error: error sending request for url (http://localhost/does_not_exist): error trying to connect: tcp connect error: Connection refused (os error 111)
#
# When run from testdrive, the command doesn't get beyond binding, and we see this error:
#  ERROR: binary encoding of list types is not implemented

# COPY FROM expressions should immediately succeed or fail on their first runs
$ set-max-tries max-tries=1

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_copy_from_remote = true;

> CREATE TABLE t1 (a bigint list);

> COPY INTO t1 FROM 'http://localhost/does_not_exist' (FORMAT PARQUET);
