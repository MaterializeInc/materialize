// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { InferResult } from "kysely";

import { queryBuilder } from "../db";
import { executeSqlV2 } from "../executeSqlV2";

export type DatabaseDetailsParameters = {
  name: string;
};

export function buildDatabaseDetailsQuery(params: { name: string }) {
  return (
    queryBuilder
      .selectFrom("mz_databases as d")
      // Due to an ID migration, some schemas won't have a matching create record
      .leftJoin("mz_object_lifetimes as ol", (join) =>
        join
          .onRef("ol.id", "=", "d.id")
          .on("ol.object_type", "=", "database")
          .on("ol.event_type", "=", "create"),
      )
      .innerJoin("mz_roles as r", "r.id", "d.owner_id")
      .select([
        "d.id",
        "d.name",
        "r.name as owner",
        "ol.occurred_at as createdAt",
      ])
      .where("d.name", "=", params.name)
  );
}

export function fetchDatabaseDetails({
  parameters,
  queryKey,
  requestOptions,
}: {
  parameters: DatabaseDetailsParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildDatabaseDetailsQuery(parameters).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

export type DatabaseDetails = InferResult<
  ReturnType<typeof buildDatabaseDetailsQuery>
>[0];
