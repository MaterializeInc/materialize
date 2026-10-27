// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { useSqlApiRequest } from "~/api/materialize";

import { buildSqlRequest, OnError, OnSettled, OnSuccess } from ".";
import { SqlRequest } from "./executeSql";

/**
 * A React hook that exposes a handler to run a SQL query against the current environment.
 * @param queryBuilder - A function that takes variables and outputs an SQL query string
 */
export function useSqlLazy<TVariables>({
  queryBuilder,
  onSuccess,
  onError,
  onSettled,
  timeout,
}: {
  queryBuilder: (variables: TVariables) => string | SqlRequest;
  onSuccess?: OnSuccess;
  onError?: OnError;
  onSettled?: OnSettled;
  timeout?: number;
}) {
  const {
    runSql: runSqlInner,
    data,
    error,
    loading,
  } = useSqlApiRequest({ timeout, lazy: true });

  const runSql = React.useCallback(
    (
      variables: TVariables,
      options?: {
        onSuccess?: OnSuccess;
        onError?: OnError;
        onSettled?: OnSettled;
      },
    ) => {
      const queryOrQueries = queryBuilder(variables);
      if (typeof queryOrQueries === "string") {
        const request = buildSqlRequest(queryOrQueries);
        runSqlInner(
          request,
          options?.onSuccess ?? onSuccess,
          options?.onError ?? onError,
          options?.onSettled ?? onSettled,
        );
      } else {
        runSqlInner(
          queryOrQueries,
          options?.onSuccess ?? onSuccess,
          options?.onError ?? onError,
          options?.onSettled ?? onSettled,
        );
      }
    },
    [queryBuilder, runSqlInner, onSuccess, onError, onSettled],
  );

  return { data, error, loading, runSql };
}
