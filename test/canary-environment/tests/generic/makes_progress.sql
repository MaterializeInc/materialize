-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

{% test makes_progress(model) %}
-- {{ model }} must be mentioned somewhere in the query or dbt will croak */

{% set query %}
   BEGIN;
   DECLARE c1 CURSOR FOR SUBSCRIBE ( SELECT * FROM {{ model }} )  WITH (SNAPSHOT = FALSE);
   FETCH 1 c1 WITH (timeout='60s');
{% endset %}

{% set result = run_query(query) %}

{% if execute %}
SELECT 1 WHERE {{ result.rows | length }} = 0
{% endif %}

-- we need to explicitly reset the current transaction
-- so that a new one is opened for the next SUBSCRIBE
{% do run_query("ROLLBACK") %}

{% endtest %}
