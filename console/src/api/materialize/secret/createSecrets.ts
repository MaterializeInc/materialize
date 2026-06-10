// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { InferResult, sql } from "kysely";

import { escapedLiteral as lit, ExecuteSqlError } from "~/api/materialize";
import {
  buildFullyQualifiedSchemaIdentifier,
  queryBuilder,
} from "~/api/materialize";
import { executeSqlV2 } from "~/api/materialize/executeSqlV2";

import { OkSqlResult } from "../types";

export type CreateSecretsError = {
  error: ExecuteSqlError;
  payloadIndex: number;
};

export type CreateSecretsSuccess = {
  secret: CreatedSecret;
  payloadIndex: number;
};

type CreateSecretInput = {
  name: string;
  value: string;
  databaseName: string;
  schemaName: string;
};

export type CreateSecretVariables = {
  name: string;
  value: string;
  databaseName: string;
  schemaName: string;
};

export function buildCreateSecretQuery(variables: CreateSecretVariables) {
  return sql<OkSqlResult>`
  CREATE SECRET ${buildFullyQualifiedSchemaIdentifier(variables)}
  AS ${lit(variables.value)}
`;
}

export async function createSecret({
  variables,
  queryKey,
  requestOptions,
}: {
  variables: CreateSecretVariables;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const compiledQuery = buildCreateSecretQuery(variables).compile(queryBuilder);

  return executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });
}

export function getSecretQueryBuilder(variables: {
  name: string;
  databaseName: string;
  schemaName: string;
}) {
  return queryBuilder
    .selectFrom("mz_object_fully_qualified_names as o")
    .where("o.database_name", "=", variables.databaseName)
    .where("o.schema_name", "=", variables.schemaName)
    .where("o.name", "=", variables.name)
    .select([
      "o.id",
      "o.name",
      "o.database_name as databaseName",
      "o.schema_name as schemaName",
    ]);
}

export const getSecretColumns = ["id", "name", "databaseName", "schemaName"];

export type CreatedSecret = InferResult<
  ReturnType<typeof getSecretQueryBuilder>
>[0];

export async function createSecrets({
  secrets,
}: {
  secrets: CreateSecretInput[];
  environmentdHttpAddress: string;
}) {
  const errors: CreateSecretsError[] = [];
  const createdSecrets: CreateSecretsSuccess[] = [];

  const results = await Promise.allSettled(
    secrets.map(({ name, value, schemaName, databaseName }) => {
      const createSecretQuery = buildCreateSecretQuery({
        name,
        value,
        schemaName,
        databaseName,
      }).compile(queryBuilder);

      const getSecretQuery = getSecretQueryBuilder({
        name,
        schemaName,
        databaseName,
      }).compile();
      return executeSqlV2({
        queries: [createSecretQuery, getSecretQuery] as const,
        queryKey: ["createSecret"],
      });
    }),
  );

  results.forEach((result, i) => {
    if (result.status === "rejected" && result.reason instanceof Error) {
      errors.push({
        payloadIndex: i,
        error: { errorMessage: result.reason.message },
      });
      return;
    } else if (result.status === "fulfilled") {
      const [_okSqlResult, newSecretResponse] = result.value;
      createdSecrets.push({
        payloadIndex: i,
        secret: newSecretResponse.rows[0],
      });
    }
  });

  return {
    data: createdSecrets,
    errors,
  };
}

export default createSecrets;
