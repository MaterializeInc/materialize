-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE TABLE build_annotation (
   build_id TEXT NOT NULL,
   build_job_id TEXT NOT NULL,
   test_suite TEXT NOT NULL,
   test_retry_count UINT4 NOT NULL,
   is_failure BOOL NOT NULL,
   insert_date TIMESTAMPTZ NOT NULL
);

CREATE TABLE build_annotation_error (
   build_job_id TEXT NOT NULL,
   error_type TEXT NOT NULL,
   content TEXT NOT NULL,
   issue TEXT,
   occurrence_count UINT4 NOT NULL
);

ALTER TABLE build_annotation OWNER TO qa;
ALTER TABLE build_annotation_error OWNER TO qa;
GRANT SELECT, INSERT, UPDATE ON TABLE build_annotation TO "hetzner-ci";
GRANT SELECT, INSERT, UPDATE ON TABLE build_annotation_error TO "hetzner-ci";
