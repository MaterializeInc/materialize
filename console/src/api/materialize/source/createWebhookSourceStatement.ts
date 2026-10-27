// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { sql } from "kysely";

import {
  buildFullyQualifiedObjectName,
  escapedLiteral as lit,
  executeSqlV2,
  queryBuilder,
} from "~/api/materialize";
import { Cluster } from "~/api/materialize/cluster/useConnectorClusters";

export type BodyFormat = "bytes" | "json" | "json_array" | "text";

export type HeaderBehavior = "all" | "include_specific" | "none";

type HeaderPayload = {
  headerBehavior: HeaderBehavior;
  headers: Array<{ header: string; column: string }>;
};

type RequestValidation = {
  validateRequests: boolean;
  checkStatement: string;
};

export type CreateWebhookSourceParameters = {
  name: string;
  databaseName: string;
  schemaName: string;
  cluster: Cluster;
  bodyFormat: BodyFormat;
} & HeaderPayload &
  RequestValidation;

function createIncludeStatement({ headerBehavior, headers }: HeaderPayload) {
  let includeSpec = sql``;
  if (headerBehavior === "all") {
    includeSpec = sql`INCLUDE HEADERS`;
  } else if (headerBehavior === "include_specific") {
    headers.forEach(({ header, column }) => {
      includeSpec = sql`${includeSpec} INCLUDE HEADER ${lit(header)} as ${sql.id(column)}`;
    });
  }
  return includeSpec;
}

function createBodyFormatStatement({
  bodyFormat,
}: CreateWebhookSourceParameters) {
  switch (bodyFormat) {
    case "json_array":
      return sql`JSON ARRAY`;
    case "json":
      return sql`JSON`;
    case "text":
      return sql`TEXT`;
    case "bytes":
      return sql`BYTES`;
    default:
      throw new Error("Unknown body format");
  }
}

export const createWebhookSourceStatement = (
  params: CreateWebhookSourceParameters,
) => {
  const name = buildFullyQualifiedObjectName(params);
  const bodyFormat = createBodyFormatStatement(params);
  const includeSpec = createIncludeStatement(params);
  const checkStatement = params.validateRequests
    ? sql.raw(params.checkStatement)
    : sql``;
  return sql`
CREATE SOURCE ${name}
IN CLUSTER ${sql.id(params.cluster.name)}
FROM WEBHOOK BODY FORMAT ${bodyFormat}
${includeSpec}
${checkStatement}
`;
};

function createWebhookSource({
  params,
  queryKey,
  requestOptions,
}: {
  params: CreateWebhookSourceParameters;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const createWebhook =
    createWebhookSourceStatement(params).compile(queryBuilder);
  return executeSqlV2({
    queries: [createWebhook],
    queryKey,
    requestOptions,
  });
}

export default createWebhookSource;
