// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  OntologyColumn,
  OntologyEdge,
  OntologyEdgeKind,
  OntologyGraph,
  OntologyNode,
} from "./model";

export interface CatalogEntityRow {
  name: string;
  relation: string;
  properties: Record<string, unknown> | null;
  description: string | null;
}

export interface CatalogLinkRow {
  name: string;
  sourceEntity: string;
  targetEntity: string;
  properties: Record<string, unknown> | null;
  description: string | null;
}

export interface CatalogPropertyRow {
  entityType: string;
  columnName: string;
  semanticType: string | null;
  description: string | null;
  /** From `mz_columns`, null when the column is not found there. */
  type: string | null;
}

const KNOWN_KINDS = new Set<OntologyEdgeKind>([
  "foreign_key",
  "maps_to",
  "measures",
  "depends_on",
  "union",
]);

const stringProperty = (
  properties: Record<string, unknown> | null,
  key: string,
): string | null => {
  const value = properties?.[key];
  return typeof value === "string" ? value : null;
};

const stringListProperty = (
  properties: Record<string, unknown> | null,
  key: string,
): string[] => {
  const value = properties?.[key];
  if (Array.isArray(value)) {
    return value.filter((v): v is string => typeof v === "string");
  }
  return typeof value === "string" ? [value] : [];
};

/** Explains the parts of a link's properties that do not fit a column pair. */
function linkNote(properties: Record<string, unknown> | null): string | null {
  const notes: string[] = [];
  const discriminatorColumn = stringProperty(
    properties,
    "discriminator_column",
  );
  const discriminatorValue = stringProperty(properties, "discriminator_value");
  if (discriminatorColumn && discriminatorValue) {
    notes.push(`Rows where ${discriminatorColumn} = '${discriminatorValue}'.`);
  }
  const fromType = stringProperty(properties, "from_type");
  const toType = stringProperty(properties, "to_type");
  if (fromType && toType) notes.push(`Maps ${fromType} to ${toType}.`);
  const metric = stringProperty(properties, "metric");
  if (metric) notes.push(`Measures ${metric}.`);
  if (properties?.nullable === true) {
    notes.push("The join column is nullable, so the relationship is optional.");
  }
  const note = stringProperty(properties, "note");
  if (note) notes.push(note);
  return notes.length > 0 ? notes.join(" ") : null;
}

/**
 * Assembles the system catalog ontology from the `mz_internal.mz_ontology_*`
 * views. Entities are grouped by the schema of the relation they describe.
 */
export function buildCatalogGraph({
  entities,
  links,
  properties,
}: {
  entities: CatalogEntityRow[];
  links: CatalogLinkRow[];
  properties: CatalogPropertyRow[];
}): OntologyGraph {
  const columnsByEntity = new Map<string, OntologyColumn[]>();
  for (const property of properties) {
    const columns = columnsByEntity.get(property.entityType) ?? [];
    columns.push({
      name: property.columnName,
      type: property.type ?? "",
      description: property.description,
      semanticType: property.semanticType,
    });
    columnsByEntity.set(property.entityType, columns);
  }

  const nodes: OntologyNode[] = entities.map((entity) => {
    const [schemaName] = entity.relation.split(".");
    return {
      id: entity.name,
      label: entity.name,
      group: schemaName,
      kind: "entity",
      relation: entity.relation,
      description: entity.description || null,
      columns: columnsByEntity.get(entity.name) ?? [],
      primaryKey: stringListProperty(entity.properties, "primary_key"),
      objectId: null,
      databaseName: null,
      schemaName,
      clusterName: null,
      isDataProduct: false,
      isUnresolved: false,
    };
  });
  const entityNames = new Set(nodes.map((node) => node.id));

  const edges = new Map<string, OntologyEdge>();
  for (const link of links) {
    if (
      !entityNames.has(link.sourceEntity) ||
      !entityNames.has(link.targetEntity)
    ) {
      continue;
    }
    const rawKind = stringProperty(link.properties, "kind");
    const kind: OntologyEdgeKind =
      rawKind && KNOWN_KINDS.has(rawKind as OntologyEdgeKind)
        ? (rawKind as OntologyEdgeKind)
        : "other";
    const sourceColumns = stringListProperty(link.properties, "source_column");
    const targetColumns = stringListProperty(link.properties, "target_column");
    const id = [
      kind,
      link.name,
      link.sourceEntity,
      link.targetEntity,
      sourceColumns.join(","),
      stringProperty(link.properties, "discriminator_value") ?? "",
      stringProperty(link.properties, "metric") ?? "",
    ].join("|");
    edges.set(id, {
      id,
      source: link.sourceEntity,
      target: link.targetEntity,
      kind,
      name: link.name,
      sourceColumns,
      targetColumns,
      description: link.description || null,
      cardinality: stringProperty(link.properties, "cardinality"),
      via:
        stringProperty(link.properties, "requires_mapping") ??
        stringProperty(link.properties, "via"),
      note: linkNote(link.properties),
      declaredIn: null,
      issues: [],
    });
  }

  return {
    nodes: nodes.sort((a, b) => a.id.localeCompare(b.id)),
    edges: [...edges.values()].sort((a, b) => a.id.localeCompare(b.id)),
    issues: [],
  };
}
