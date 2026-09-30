-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE SOURCE cdc_source
    IN CLUSTER ingest
    FROM KAFKA CONNECTION app.public.kafka_conn (TOPIC 'cdc_bare_ledger_entry');
