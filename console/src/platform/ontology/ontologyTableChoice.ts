// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  CatalogRelation,
  displayName,
  parseQualifiedName,
} from "./dataProductGraph";

/** The table declared joins are read from, unless the user picks another. */
export const DEFAULT_ONTOLOGY_TABLE = "public.ontology";

/** The database an unqualified default resolves in when several match. */
const DEFAULT_DATABASE = "materialize";

/**
 * The relation a typed name refers to. A name matching relations in several
 * databases prefers the default database, then the first by name.
 */
export function resolveOntologyTable(
  name: string,
  relations: CatalogRelation[],
): CatalogRelation | null {
  const parts = parseQualifiedName(name);
  if (!parts) return null;
  const [item, schema, database] = [...parts].reverse();
  const matches = relations
    .filter(
      (relation) =>
        relation.name === item &&
        (schema === undefined || relation.schemaName === schema) &&
        (database === undefined || relation.databaseName === database),
    )
    .sort((a, b) => displayName(a).localeCompare(displayName(b)));
  return (
    matches.find((relation) => relation.databaseName === DEFAULT_DATABASE) ??
    matches[0] ??
    null
  );
}

/**
 * Scores `text` against `query` as an in-order subsequence, or returns null
 * when it is not one. Consecutive matches and matches in the last name part
 * score higher, so `ontology` ranks `auction.ontology` above
 * `orders_to_logs_by_yday`.
 */
export function fuzzyScore(query: string, text: string): number | null {
  const needle = query.trim().toLowerCase();
  const haystack = text.toLowerCase();
  if (needle === "") return 0;
  const lastPart = haystack.lastIndexOf(".") + 1;
  let score = 0;
  let previous = -2;
  let from = 0;
  for (const ch of needle) {
    const at = haystack.indexOf(ch, from);
    if (at === -1) return null;
    score += at === previous + 1 ? 3 : 1;
    if (at >= lastPart) score += 1;
    previous = at;
    from = at + 1;
  }
  return score;
}

export interface OntologyTableCandidate {
  relation: CatalogRelation;
  name: string;
  /** Has the four ontology table columns. */
  hasShape: boolean;
}

const MAX_CANDIDATES = 50;

/**
 * Relations the picker offers for `query`: those with the ontology table's
 * columns first, then by match quality.
 */
export function rankOntologyTables(
  query: string,
  relations: CatalogRelation[],
  shapeIds: ReadonlySet<string>,
): OntologyTableCandidate[] {
  return relations
    .flatMap((relation) => {
      const name = displayName(relation);
      const score = fuzzyScore(query, name);
      return score === null
        ? []
        : [{ relation, name, hasShape: shapeIds.has(relation.id), score }];
    })
    .sort(
      (a, b) =>
        Number(b.hasShape) - Number(a.hasShape) ||
        b.score - a.score ||
        a.name.localeCompare(b.name),
    )
    .slice(0, MAX_CANDIDATES)
    .map(({ relation, name, hasShape }) => ({ relation, name, hasShape }));
}
