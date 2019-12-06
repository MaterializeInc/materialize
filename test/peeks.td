# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

# This file tests peek processing, and that it does not create dataflows
# in cases where we can streamline the processing. The mechanism it uses
# for this is to create a view which should have a known dataflow index,
# because we should not have created any views in the course of all of
# these peeks.

> SELECT * FROM logs_addresses

> SELECT id FROM logs_addresses

> SELECT * FROM logs_addresses WHERE logs_addresses.worker = 0

> SELECT id FROM logs_addresses WHERE logs_addresses.worker = 0

> SELECT id FROM logs_addresses WHERE logs_addresses.worker = 0 ORDER BY id

# At this point we should have installed no views; this should be our first.
> CREATE VIEW test AS SELECT * FROM logs_addresses;

# We should only find one top level slot.
> SELECT COUNT(*) FROM test WHERE slot = 0 and value != 3
0
