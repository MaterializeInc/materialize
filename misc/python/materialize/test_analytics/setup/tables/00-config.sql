-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE TABLE config (
   uploads_enabled BOOL NOT NULL,
   min_required_data_version_for_uploads INT NOT NULL,
   only_notify_about_communication_failures_on_main BOOL NOT NULL
);

INSERT INTO config
(
    uploads_enabled,
    min_required_data_version_for_uploads,
    only_notify_about_communication_failures_on_main
)
VALUES
(
    TRUE,
    6,
    FALSE
);

CREATE ROLE qa;
GRANT qa TO "dennis.felsing@materialize.com";
ALTER TABLE config OWNER TO qa;
GRANT SELECT ON TABLE config TO "hetzner-ci";
