-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- The changefeed emits a bare envelope: one Avro field named `record` holding
-- the row. ENVELOPE NONE therefore yields a single column of that name, typed
-- as an anonymous record.
CREATE TABLE ledger_entry
    FROM SOURCE app.ingest.cdc_source (REFERENCE "cdc_bare_ledger_entry")
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION app.public.csr_conn
    ENVELOPE NONE;
