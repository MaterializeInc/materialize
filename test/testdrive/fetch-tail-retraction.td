
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Observe the retraction of records from a TAIL stream
#

$ set-regex match=\d{13} replacement=<TIMESTAMP>

> CREATE TABLE inserts (f1 INTEGER);

> INSERT INTO inserts VALUES (123),(123),(NULL);

> CREATE TABLE deletes (f1 INTEGER);

> CREATE MATERIALIZED VIEW v1 AS SELECT * FROM inserts EXCEPT ALL SELECT * FROM deletes;

> SELECT * FROM v1;
123
123
<null>

> BEGIN

> DECLARE c CURSOR FOR TAIL v1;

> FETCH 2 c;
<TIMESTAMP> 2 123
<TIMESTAMP> 1 <null>

#
# Force a retraction by performing an update outside of the transaction and making sure
# that the update has been fully ingested in v1.
#
# The value 999 should not cause anything to be retracted, is inserted to check
# that no erroneous retractions will be issued.
#

$ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
INSERT INTO deletes VALUES (123), (123), (NULL), (999);
SELECT * FROM v1;

> FETCH ALL c;
<TIMESTAMP> -2 123
<TIMESTAMP> -1 <null>
