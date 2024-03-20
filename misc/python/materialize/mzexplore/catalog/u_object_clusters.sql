-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Select distinct clusters associated with object_ids identifying either an
-- index or a materialized view.
WITH
  -- Pre-define a map from dataflow-backed catalog items to clusters.
  object_clusters(object_id, cluster_id) AS (
    SELECT id, cluster_id FROM mz_catalog.mz_indexes
    UNION
    SELECT id, cluster_id FROM mz_catalog.mz_materialized_views
  )
  -- Select distinct clusters.
  SELECT DISTINCT
    c.id,
    c.name
  FROM
    object_clusters oc JOIN
    mz_clusters c ON(oc.cluster_id = c.id)
  WHERE
    object_id IN ({object_ids});
