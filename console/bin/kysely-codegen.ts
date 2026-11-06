// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Generates types/materialize.d.ts from a running Materialize. Run through
// bin/generate-types, which starts one.

import { writeFileSync } from "node:fs";

import { type ColumnMetadata, sql, type TableMetadata } from "kysely";
import {
  generate,
  type IntrospectOptions,
  Logger,
  type PostgresDB,
  PostgresDialect,
  PostgresIntrospector,
} from "kysely-codegen";

const URL = "postgres://localhost:6875/materialize";
const OUT_FILE = "types/materialize.d.ts";
// Must correspond with the search path set in executeSql.
const SCHEMAS = [
  "mz_catalog",
  "mz_catalog_unstable",
  "mz_internal",
  "mz_introspection",
];

const HEADER = `// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

`;

type ColumnRow = {
  schema: string;
  table: string;
  relkind: string;
  column: string;
  type: string;
  typeSchema: string;
  notNull: boolean;
  hasDefault: boolean;
  comment: string | null;
};

/**
 * kysely-codegen's Postgres introspector with a table query Materialize can run.
 */
class MaterializeIntrospector extends PostgresIntrospector {
  // The inherited query is kysely's, which calls `pg_get_serial_sequence`
  // (missing in Materialize) and lists only relkinds 'r', 'v' and 'p', which
  // skips the catalog's builtin materialized views ('m'). No column is
  // auto-incrementing, since Materialize's pg_class reports no sequences
  // (relkind 'S'). Ignores `includePattern` and `excludePattern`.
  protected async getTables({
    db,
  }: IntrospectOptions<PostgresDB>): Promise<TableMetadata[]> {
    const { rows } = await sql<ColumnRow>`
      SELECT
        ns.nspname AS "schema",
        c.relname AS "table",
        c.relkind AS "relkind",
        a.attname AS "column",
        typ.typname AS "type",
        dtns.nspname AS "typeSchema",
        a.attnotnull AS "notNull",
        a.atthasdef AS "hasDefault",
        col_description(a.attrelid, a.attnum) AS "comment"
      FROM pg_catalog.pg_attribute AS a
      JOIN pg_catalog.pg_class AS c ON a.attrelid = c.oid
      JOIN pg_catalog.pg_namespace AS ns ON c.relnamespace = ns.oid
      JOIN pg_catalog.pg_type AS typ ON a.atttypid = typ.oid
      JOIN pg_catalog.pg_namespace AS dtns ON typ.typnamespace = dtns.oid
      WHERE c.relkind IN ('r', 'v', 'm')
        AND ns.nspname !~ '^pg_'
        AND ns.nspname <> 'information_schema'
        AND has_schema_privilege(ns.nspname, 'USAGE')
        AND a.attnum >= 0
        AND NOT a.attisdropped
      ORDER BY ns.nspname, c.relname, a.attnum
    `.execute(db);

    const tables = new Map<
      string,
      TableMetadata & { columns: ColumnMetadata[] }
    >();
    for (const row of rows) {
      const key = `${row.schema}.${row.table}`;
      let table = tables.get(key);
      if (!table) {
        table = {
          name: row.table,
          schema: row.schema,
          isView: row.relkind !== "r",
          columns: [],
        };
        tables.set(key, table);
      }
      table.columns.push({
        name: row.column,
        dataType: row.type,
        dataTypeSchema: row.typeSchema,
        isNullable: !row.notNull,
        hasDefaultValue: row.hasDefault,
        isAutoIncrementing: false,
        comment: row.comment ?? undefined,
      });
    }
    return [...tables.values()];
  }
}

class MaterializeDialect extends PostgresDialect {
  readonly introspector = new MaterializeIntrospector({ domains: false });
}

const dialect = new MaterializeDialect();
const db = await dialect.introspector.connect({
  connectionString: URL,
  dialect,
});
try {
  const types = await generate({
    db,
    dialect,
    logger: new Logger(),
    schemas: SCHEMAS,
  });
  writeFileSync(OUT_FILE, HEADER + types);
  console.log(`Wrote ${OUT_FILE}`);
} finally {
  await db.destroy();
}
