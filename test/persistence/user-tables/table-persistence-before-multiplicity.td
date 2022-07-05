# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that diffs > 1 are handled correctly
#

> CREATE TABLE multiplicity_table (f1 INTEGER);

> INSERT INTO multiplicity_table VALUES (NULL), (NULL), (1), (1);

> INSERT INTO multiplicity_table VALUES (NULL), (NULL), (1), (1);
