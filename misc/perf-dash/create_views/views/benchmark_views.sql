-- Copyright Materialize, Inc. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE SOURCE benchmark_results_v0
FROM KAFKA BROKER 'localhost:9093'
TOPIC 'dev.mtrlz.benchmarks.results.v0'
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://localhost:8081'
ENVELOPE UPSERT
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://localhost:8081';

CREATE MATERIALIZED VIEW benchmark_results AS SELECT
    benchmark_id,
    git_ref,
    mz_workers,
    passed,
    rows_per_second,
    start_ms,
    end_ms
FROM benchmark_results_v0;
