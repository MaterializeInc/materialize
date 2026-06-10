-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE TABLE issue (
    issue_id TEXT NOT NULL,
    title TEXT NOT NULL,
    ci_regexp TEXT NOT NULL,
    state TEXT NOT NULL
);

ALTER TABLE issue OWNER TO qa;
GRANT SELECT, INSERT, UPDATE ON TABLE issue TO "hetzner-ci";
