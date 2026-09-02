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

import { executeSqlV2, queryBuilder } from "~/api/materialize";

export const buildStatementLoggingMaxSampleRateQuery = () => {
  return sql<{
    statement_logging_max_sample_rate: string;
  }>`SHOW statement_logging_max_sample_rate`;
};

/**
 * Fetches the system-wide cap on the statement logging sample rate. The effective rate is
 * `min(session statement_logging_sample_rate, this)`, so a value of `0` means statement
 * logging is off for everyone and query history can never have rows.
 *
 * Returns `null` if the variable could not be read as a number.
 */
export default async function fetchStatementLoggingMaxSampleRate({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions: RequestInit;
}) {
  const compiledQuery =
    buildStatementLoggingMaxSampleRateQuery().compile(queryBuilder);

  const response = await executeSqlV2({
    queries: compiledQuery,
    queryKey,
    requestOptions,
  });

  const rate = parseFloat(response.rows[0]?.statement_logging_max_sample_rate);

  return Number.isFinite(rate) ? rate : null;
}
