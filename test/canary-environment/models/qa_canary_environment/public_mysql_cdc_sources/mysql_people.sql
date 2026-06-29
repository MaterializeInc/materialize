-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE TABLE mysql_people FROM SOURCE qa_canary_environment.public_mysql_cdc_sources.mysql_cdc (REFERENCE "public"."people");
