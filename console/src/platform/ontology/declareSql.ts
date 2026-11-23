// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ONTOLOGY_TABLE_COLUMNS } from "~/api/materialize/ontology/ontology";

import { displayIdent, qualifiedName } from "./dataProductGraph";
import { OntologyEdge, OntologyNode } from "./model";

const literal = (value: string) => `'${value.replace(/'/g, "''")}'`;
const arrayLiteral = (values: string[]) =>
  `ARRAY[${values.map(literal).join(", ")}]`;

/** Where declared joins go: the chosen ontology table, or where to create it. */
export interface OntologyTableHome {
  databaseName: string | null;
  schemaName: string;
  name: string;
  exists: boolean;
}

/**
 * A relation's name as an ontology table row should hold it: as short as
 * resolving it from the table's schema allows (see `resolveRelationName`).
 */
function nameFrom(
  node: OntologyNode,
  table: { databaseName: string | null; schemaName: string },
) {
  const parts =
    node.databaseName !== table.databaseName
      ? [node.databaseName, node.schemaName, node.label]
      : node.schemaName !== table.schemaName
        ? [node.schemaName, node.label]
        : [node.label];
  return parts
    .filter((part): part is string => part !== null)
    .map(displayIdent)
    .join(".");
}

/**
 * Writes the SQL that declares a join in the ontology table, creating the
 * table first when it does not exist.
 */
export function buildDeclareSql(
  edge: OntologyEdge,
  source: OntologyNode,
  target: OntologyNode,
  home: OntologyTableHome,
): string | null {
  if (
    !source.schemaName ||
    !target.schemaName ||
    edge.sourceColumns.length === 0 ||
    edge.sourceColumns.length !== edge.targetColumns.length
  ) {
    return null;
  }
  const tableName = qualifiedName(home);
  const insert = [
    `INSERT INTO ${tableName} VALUES (`,
    `  ${literal(nameFrom(source, home))}, ${arrayLiteral(edge.sourceColumns)},`,
    `  ${literal(nameFrom(target, home))}, ${arrayLiteral(edge.targetColumns)}`,
    `);`,
  ].join("\n");
  if (home.exists) return insert;
  return [
    `CREATE TABLE ${tableName} (`,
    ONTOLOGY_TABLE_COLUMNS.map(
      ([name, type]) => `  ${name} ${type} NOT NULL`,
    ).join(",\n"),
    `);`,
    `CREATE DEFAULT INDEX ON ${tableName};`,
    insert,
  ].join("\n");
}
