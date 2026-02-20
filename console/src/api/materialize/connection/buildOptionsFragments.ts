// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";

import { buildFullyQualifiedObjectName, escapedLiteral as lit } from "..";
import { Secret, TextSecret } from "./types";

/**
 * Returns a RawBuilder that compiles to a sql OPTIONS (...) string.
 * Keys must not come from user input, since they are included as raw values.
 */
export function buildOptionsFragments(
  options: [string, string | Secret | TextSecret | undefined][],
) {
  return options
    .filter(([_, value]) => Boolean(value))
    .map(([key, value]) => {
      // PORT is an integer and we shouldn't have quotations around it
      if (key === "PORT" && typeof value === "string") {
        return sql`${sql.raw(key)} ${sql.raw(value)}`;
      }

      if (typeof value === "string") {
        return sql`${sql.raw(key)} ${lit(value)}`;
      }

      if ("secretTextValue" in value!) {
        return sql`${sql.raw(key)} ${lit(value.secretTextValue)}`;
      }

      if ("secretName" in value!) {
        const secret = buildFullyQualifiedObjectName({
          name: value.secretName,
          databaseName: value.databaseName,
          schemaName: value.schemaName,
        });
        return sql`${sql.raw(key)} SECRET ${secret}`;
      }

      return null;
    });
}
