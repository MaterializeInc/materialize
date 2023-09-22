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
  w2.seller,
  w2.item AS seller_item,
  w2.amount AS seller_amount,
  w1.item buyer_item,
  w1.amount buyer_amount
FROM {{ ref('winning_bids') }} w1
JOIN {{ ref('winning_bids') }} w2 ON w1.buyer = w2.seller
WHERE w2.amount > w1.amount
