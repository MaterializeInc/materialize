// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { mapSqlCode, splitSql, trimStatement } from "./sqlTokens";

/** Cell names are lowercase so the unquoted alias they expand to matches them. */
export const CELL_NAME_REGEX = /^[a-z_][a-z0-9_]*$/;

/** Names the Observable runtime defines itself, so a cell cannot take them. */
export const RESERVED_CELL_NAMES = new Set(["invalidation", "visibility"]);

const CELL_REFERENCE_REGEX = /\{\{\s*([a-z_][a-z0-9_]*)\s*\}\}/g;

/** What a referenced name resolves to: the SQL to splice in, or why not. */
export type ResolvedReference = { source: string } | { error: string };

export type ExpandResult =
  | { sql: string; dependencies: string[]; error?: undefined }
  | { sql?: undefined; dependencies: string[]; error: string };

/**
 * Replaces each `{{name}}` reference outside comments and quotes with the
 * referenced cell's SQL as a subquery aliased to `name`, recursively.
 *
 * `dependencies` lists every transitively referenced cell.
 */
export function expandCellReferences(
  sql: string,
  resolve: (name: string) => ResolvedReference,
): ExpandResult {
  const dependencies = new Set<string>();

  const expand = (body: string, stack: string[]): string =>
    mapSqlCode(trimStatement(body), (code) =>
      code.replace(CELL_REFERENCE_REGEX, (_match, name: string) => {
        if (stack.includes(name)) {
          throw new Error(
            `Cell reference cycle: ${[...stack, name].join(" → ")}`,
          );
        }
        const resolved = resolve(name);
        if ("error" in resolved) throw new Error(resolved.error);
        dependencies.add(name);
        // The body sits on its own lines so nothing after it can be pulled
        // into a comment it ends with.
        return `(\n${expand(resolved.source, [...stack, name])}\n) AS ${name}`;
      }),
    );

  try {
    return { sql: expand(sql, []), dependencies: Array.from(dependencies) };
  } catch (e) {
    return {
      error: e instanceof Error ? e.message : String(e),
      dependencies: Array.from(dependencies),
    };
  }
}

/** The names `sql` references directly, outside comments and quotes. */
export function findCellReferences(sql: string): string[] {
  return splitSql(sql)
    .filter((s) => s.kind === "code")
    .flatMap((s) =>
      Array.from(s.text.matchAll(CELL_REFERENCE_REGEX), (m) => m[1]),
    );
}

/** Rewrites references to `from` as references to `to`, outside comments and quotes. */
export function renameCellReferences(sql: string, from: string, to: string) {
  const reference = new RegExp(String.raw`\{\{\s*${from}\s*\}\}`, "g");
  return mapSqlCode(sql, (code) => code.replace(reference, `{{${to}}}`));
}
