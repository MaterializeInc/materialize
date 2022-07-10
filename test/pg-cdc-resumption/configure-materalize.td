# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> DROP SOURCE IF EXISTS mz_source CASCADE;
> DROP SECRET IF EXISTS pgpass CASCADE;
> DROP CONNECTION IF EXISTS pg CASCADE;

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg FOR POSTGRES
  HOST postgres,
  DATABASE postgres,
  USER postgres,
  PASSWORD SECRET pgpass


> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES
  CONNECTION pg
  PUBLICATION 'mz_source'

> DROP SOURCE IF EXISTS mz_source_alter CASCADE;

> CREATE MATERIALIZED SOURCE mz_source_alter
  FROM POSTGRES
  CONNECTION pg
  PUBLICATION 'mz_source_alter'

> CREATE MATERIALIZED VIEWS FROM SOURCE mz_source;

> CREATE MATERIALIZED VIEWS FROM SOURCE mz_source_alter;
