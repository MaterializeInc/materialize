-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License in the LICENSE file at the
-- root of this repository, or online at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

{{ config(materialized='view') }}

SELECT
  id,
  SUM(credits) as credits,
  SUM(debits) as debits
FROM (
  SELECT seller as id, amount as credits, 0 as debits
  FROM {{ ref('winning_bids') }}
  UNION ALL
  SELECT buyer as id, 0 as credits, amount as debits
  FROM {{ ref('winning_bids') }}
)
GROUP BY id
