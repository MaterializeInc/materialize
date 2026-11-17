# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> UPDATE pgsink_input SET b = 'recovered' WHERE a = 1
> DELETE FROM pgsink_input WHERE a = 3
> INSERT INTO pgsink_input VALUES (4, 'four')

> SELECT name, status FROM mz_internal.mz_sink_statuses WHERE name LIKE 'pgsink_%'
pgsink_keyed stalled
pgsink_keyless stalled
