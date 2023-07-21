# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-connect name=materialize url=postgres://materialize:materialize@${testdrive.materialize-sql-addr}

# In v0.62+, `returning` must be quoted when used as an identifier.
$ postgres-execute connection=materialize
CREATE VIEW view_with_returning AS SELECT "returning" FROM (VALUES (1)) _ ("returning")
