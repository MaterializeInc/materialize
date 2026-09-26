# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT status FROM mz_internal.mz_sink_statuses
  WHERE id = (SELECT id FROM mz_sinks WHERE name = 'overlap_sink')
running

$ duckdb-execute name=overlap
CREATE SECRET s3_secret (TYPE S3, KEY_ID 'tduser', SECRET '${arg.s3-access-key}', ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false, REGION 'minio');
CREATE VIEW committed AS SELECT * FROM iceberg_scan('${arg.metadata-location}');

# No aggregation or deduplication may hide replayed rows.
$ duckdb-query name=overlap
SELECT id, val, _mz_diff FROM committed ORDER BY id
1 a 1
2 b 1
3 c 1
4 d 1
5 e 1
6 f 1

$ duckdb-query name=overlap
SELECT count(*), count(*) FILTER (WHERE id <= 3 AND _mz_timestamp < ${arg.injected-upper}), count(*) FILTER (WHERE id > 3 AND _mz_timestamp > ${arg.injected-upper} AND _mz_timestamp <= ${arg.final-read}) FROM committed
6 3 3
