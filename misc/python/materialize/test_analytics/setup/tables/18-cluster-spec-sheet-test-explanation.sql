-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.


-- What each `test_name` in the cluster spec sheet result tables measures, for
-- display next to its chart. Metadata about the test names rather than about a
-- build, so rows carry no build job reference. The `explanation` argument of
-- each measurement in test/cluster-spec-sheet/mzcompose.py is the source of
-- truth and default branch runs upsert it, keyed by `test_name` (at most one
-- row per name).
--
-- `build_number` is the build whose text a row currently holds, a high
-- watermark rather than a reference into `build`, kept so that a write from an
-- older build is dropped instead of reverting the text. Spec sheet jobs run for
-- hours and several builds are in flight at once, so they finish out of order.
-- Buildkite numbers builds per pipeline, which is comparable here only because
-- the composition runs in one pipeline (ci/spec-sheet/pipeline.template.yml).
CREATE TABLE cluster_spec_sheet_test_explanation (
   test_name TEXT NOT NULL,
   explanation TEXT NOT NULL,
   build_number UINT4 NOT NULL
);

ALTER TABLE cluster_spec_sheet_test_explanation OWNER TO qa;
GRANT SELECT, INSERT, UPDATE ON TABLE cluster_spec_sheet_test_explanation TO "hetzner-ci";
