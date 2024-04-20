# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# COPY TO expressions should immediately succeed or fail on their first runs
$ set-max-tries max-tries=1

> CREATE SCHEMA tpch1gb
> CREATE SOURCE tpch1gb.source FROM LOAD GENERATOR TPCH (SCALE FACTOR 1) FOR ALL TABLES

> COPY tpch1gb.customer TO 's3://copytos3/test/tpch1gb/customer'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch1gb.lineitem TO 's3://copytos3/test/tpch1gb/lineitem'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch1gb.nation TO 's3://copytos3/test/tpch1gb/nation'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch1gb.orders TO 's3://copytos3/test/tpch1gb/orders'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch1gb.part TO 's3://copytos3/test/tpch1gb/part'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch1gb.partsupp TO 's3://copytos3/test/tpch1gb/partsupp'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch1gb.region TO 's3://copytos3/test/tpch1gb/region'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch1gb.supplier TO 's3://copytos3/test/tpch1gb/supplier'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
