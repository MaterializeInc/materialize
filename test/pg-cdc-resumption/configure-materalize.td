# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> DROP SECRET IF EXISTS pgpass CASCADE;
> DROP CONNECTION IF EXISTS pg CASCADE;

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST toxiproxy,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  );

> DROP SOURCE IF EXISTS mz_source CASCADE;
> DROP SOURCE IF EXISTS mz_source_alter_add_col CASCADE;
> DROP SOURCE IF EXISTS mz_source_alter_drop_constraint CASCADE;

> CREATE SOURCE mz_source
  FROM POSTGRES
  CONNECTION pg
  (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> CREATE SOURCE mz_source_alter_add_col
  FROM POSTGRES
  CONNECTION pg
  (PUBLICATION 'mz_source_alter_add_col')
  FOR ALL TABLES;

> CREATE SOURCE mz_source_alter_drop_constraint
  FROM POSTGRES
  CONNECTION pg
  (PUBLICATION 'mz_source_alter_drop_constraint')
  FOR ALL TABLES;
