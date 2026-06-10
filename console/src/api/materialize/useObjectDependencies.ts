// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { DeletableObjectType } from "./buildDropObjectStatement";
import { queryBuilder } from "./db";
import useSqlTyped from "./useSqlTyped";

/**
 * Because clusters are not included in mz_object_transitive_dependencies, we have to
 * manually join all the different system tables for object types that depend on clusters.
 */
export function buildClusterDependenciesQuery(clusterId: string) {
  return queryBuilder
    .with("direct_dependencies", (qb) =>
      qb
        .selectFrom((eb) =>
          eb
            .selectFrom("mz_sinks as s")
            .where("s.id", "like", "u%")
            .select(["id", "cluster_id"])
            .unionAll(
              eb
                .selectFrom("mz_sources as s")
                .where("s.id", "like", "u%")
                .where("s.cluster_id", "is not", null)
                .select(["id", "cluster_id"])
                .$narrowType<{ cluster_id: string }>(),
            )
            .unionAll(
              eb
                .selectFrom("mz_materialized_views as mv")
                .where("mv.id", "like", "u%")
                .select(["id", "cluster_id"]),
            )
            .unionAll(
              eb
                .selectFrom("mz_indexes as i")
                .where("i.id", "like", "u%")
                .select(["id", "cluster_id"]),
            )
            .as("objects"),
        )
        .select(["id", "cluster_id"]),
    )
    .with("transitive_dependencies", (qb) =>
      qb
        .selectFrom("mz_object_transitive_dependencies")
        .select("object_id as id")
        .where("object_id", "is not", null)
        .$narrowType<{ id: string }>()
        .where("referenced_object_id", "in", (eb) =>
          eb
            .selectFrom("direct_dependencies")
            .select("id")
            .where("cluster_id", "=", clusterId),
        ),
    )
    .with("all", (qb) =>
      qb
        .selectFrom("direct_dependencies as d")
        .select("d.id")
        .where("d.cluster_id", "=", clusterId)
        .union(qb.selectFrom("transitive_dependencies as t").select("t.id")),
    )
    .selectFrom("all")
    .select((eb) => eb.fn.count<bigint>("id").as("count"))
    .compile();
}

export function buildObjectDependenciesQuery(objectId: string) {
  return queryBuilder
    .selectFrom("mz_object_transitive_dependencies")
    .select((eb) => eb.fn.count<bigint>("referenced_object_id").as("count"))
    .where("referenced_object_id", "=", objectId)
    .compile();
}

export function buildSchemaDependenciesQuery(schemaId: string) {
  return queryBuilder
    .selectFrom("mz_objects")
    .select((eb) => eb.fn.count<bigint>("id").as("count"))
    .where("schema_id", "=", schemaId)
    .compile();
}

/**
 * Fetches the number of dependencies an object has
 */
function useObjectDependencies(
  objectId: string,
  objectType: DeletableObjectType,
) {
  const query = React.useMemo(() => {
    switch (objectType) {
      case "CLUSTER REPLICA":
        return null;
      case "CLUSTER":
        return buildClusterDependenciesQuery(objectId);
      case "SCHEMA":
        return buildSchemaDependenciesQuery(objectId);
      default:
        return buildObjectDependenciesQuery(objectId);
    }
  }, [objectId, objectType]);

  const response = useSqlTyped(query);
  if (objectType === "CLUSTER REPLICA") {
    // slightly hacky, but replicas never have dependencies, so there is no query to run in this case.
    return {
      ...response,
      data: BigInt(0),
      results: BigInt(0),
    };
  }

  let count: null | bigint = null;
  if (response.results && response.results.length > 0) {
    count = response.results[0].count;
  }
  return { ...response, data: count, results: count };
}

export default useObjectDependencies;
