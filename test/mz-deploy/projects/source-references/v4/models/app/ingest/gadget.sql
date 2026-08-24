-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- `public.gadget` is a misspelling of the upstream `public.gadgets`, so the
-- check must refuse it and point at the name it came close to.
CREATE TABLE gadget FROM SOURCE app.ingest.pg_source (REFERENCE public.gadget)
