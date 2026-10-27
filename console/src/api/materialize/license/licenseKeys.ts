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

import { executeSqlV2 } from "..";
import { queryBuilder } from "../db";

export function buildLicenseKeysQuery() {
  const qb = queryBuilder
    .selectFrom("mz_license_keys")
    .selectAll()
    .orderBy("expiration", "desc")
    .limit(1);
  return qb;
}

export async function fetchLicenseKeys({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions: RequestInit;
}) {
  const compiledQuery = buildLicenseKeysQuery().compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

export type LicenseKey = InferResult<
  ReturnType<typeof buildLicenseKeysQuery>
>[0];
