-- Copyright Materialize, Inc. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE SOURCE benchmark_run_params_v0
FROM KAFKA BROKER 'kafka:9093'
TOPIC 'dev.mtrlz.benchmarks.runs.params.v0'
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
ENVELOPE UPSERT
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081';

CREATE SOURCE benchmark_run_begin_v0
FROM KAFKA BROKER 'kafka:9093'
TOPIC 'dev.mtrlz.benchmarks.runs.begin.v0'
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
ENVELOPE UPSERT
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081';

CREATE SOURCE benchmark_run_results_v0
FROM KAFKA BROKER 'kafka:9093'
TOPIC 'dev.mtrlz.benchmarks.runs.results.v0'
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
ENVELOPE UPSERT
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081';

CREATE OR REPLACE MATERIALIZED VIEW benchmark_progress AS
    SELECT bp.benchmark_id,
           bb.run_id,
           bb.start_ms,
        CASE WHEN br.end_ms IS NOT NULL THEN br.end_ms - bb.start_ms ELSE null END AS runtime,
           bp.git_ref,
           bp.mz_workers,
           br.end_ms,
        CASE WHEN br.result IS NOT NULL THEN br.result ELSE 'running' END AS result
    FROM benchmark_run_begin_v0 AS bb
    INNER JOIN benchmark_run_params_v0 AS bp ON bb.run_id = bp.run_id
    LEFT JOIN benchmark_run_results_v0 AS br ON bb.run_id = br.run_id;

CREATE OR REPLACE MATERIALIZED VIEW benchmark_results AS
    SELECT bp.benchmark_id,
           bb.run_id,
           bp.git_ref,
           bp.mz_workers,
           br.result,
           br.rows_per_second
    FROM benchmark_run_begin_v0 AS bb
    INNER JOIN benchmark_run_params_v0 AS bp ON bb.run_id = bp.run_id
    INNER JOIN benchmark_run_results_v0 AS br ON bb.run_id = br.run_id;
