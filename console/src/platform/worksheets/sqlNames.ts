// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/** The database the worksheet's sessions start in. */
const SESSION_DATABASE = "materialize";
/** Schemas whose relations resolve without a schema prefix. */
const UNQUALIFIED_SCHEMAS = new Set(["mz_catalog", "pg_catalog"]);
/** Quotes an identifier unless it would parse back unchanged. */
export function quoteIdentifier(name: string) {
  return /^[a-z_][a-z0-9_$]*$/.test(name)
    ? name
    : `"${name.replace(/"/g, '""')}"`;
}

/**
 * The shortest name that resolves to the object from a worksheet session:
 * unqualified on the search path, schema-qualified in the session database,
 * and fully qualified elsewhere.
 */
export function shortestName(
  databaseName: string | null,
  schemaName: string,
  name: string,
) {
  const parts =
    databaseName === null
      ? UNQUALIFIED_SCHEMAS.has(schemaName)
        ? [name]
        : [schemaName, name]
      : databaseName === SESSION_DATABASE
        ? schemaName === "public"
          ? [name]
          : [schemaName, name]
        : [databaseName, schemaName, name];
  return parts.map(quoteIdentifier).join(".");
}
