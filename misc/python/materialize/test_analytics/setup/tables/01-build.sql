-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- meta data of the build
CREATE TABLE build (
   pipeline TEXT NOT NULL,
   build_number UINT4 NOT NULL,
   build_id TEXT NOT NULL,
   branch TEXT NOT NULL,
   commit_hash TEXT NOT NULL,
   main_ancestor_commit_hash TEXT, -- nullable for now not to break earlier versions
   mz_version TEXT NOT NULL,
   date TIMESTAMPTZ NOT NULL,
   data_version UINT4 NOT NULL
);

CREATE INDEX IN CLUSTER test_analytics ON build (build_id);

ALTER TABLE build OWNER TO qa;
GRANT SELECT, INSERT, UPDATE ON TABLE build TO "hetzner-ci";
