// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ThemeColors } from "~/theme";

import { OntologyEdgeKind } from "./model";

export const SIDE_PANEL_WIDTH = 420;

export interface EdgeKindStyle {
  label: string;
  description: string;
  color: string;
  dash: string | undefined;
  /** The dash pattern as a CSS border style, for legends. */
  borderStyle: "solid" | "dashed" | "dotted";
}

export function edgeKindStyle(
  kind: OntologyEdgeKind,
  colors: ThemeColors,
): EdgeKindStyle {
  const style = kindStyle(kind, colors);
  const borderStyle = !style.dash
    ? "solid"
    : parseFloat(style.dash) <= 2
      ? "dotted"
      : "dashed";
  return { ...style, borderStyle };
}

// The join kinds share a color, and their line says how sure the join is.
function kindStyle(
  kind: OntologyEdgeKind,
  colors: ThemeColors,
): Omit<EdgeKindStyle, "borderStyle"> {
  switch (kind) {
    case "join":
      return {
        label: "Declared join",
        description: "Listed in an ontology table",
        color: colors.accent.purple,
        dash: undefined,
      };
    case "inferred_join":
      return {
        label: "Inferred join",
        description: "A view or materialized view joins on these columns",
        color: colors.accent.purple,
        dash: "7 4",
      };
    case "same_values":
      return {
        label: "Same values",
        description: "A view passes the column through unchanged",
        color: colors.accent.purple,
        dash: "1.5 4",
      };
    case "lineage":
      return {
        label: "Lineage",
        description: "Derived from, by the object's definition",
        color: colors.foreground.tertiary,
        dash: "6 5",
      };
    case "foreign_key":
      return {
        label: "Foreign key",
        description: "Joins on a key column",
        color: colors.accent.purple,
        dash: undefined,
      };
    case "maps_to":
      return {
        label: "Maps to",
        description: "Translates between id domains",
        color: colors.accent.orange,
        dash: "2 4",
      };
    case "measures":
      return {
        label: "Measures",
        description: "Records a metric about",
        color: colors.accent.green,
        dash: undefined,
      };
    case "depends_on":
      return {
        label: "Depends on",
        description: "References by dependency",
        color: colors.accent.blue,
        dash: undefined,
      };
    case "union":
      return {
        label: "Union member",
        description: "Included in a union view",
        color: colors.foreground.tertiary,
        dash: "6 5",
      };
    default:
      return {
        label: "Other",
        description: "Another relationship",
        color: colors.foreground.secondary,
        dash: undefined,
      };
  }
}

const OBJECT_TYPE_LABELS: Record<string, string> = {
  table: "Table",
  view: "View",
  "materialized-view": "Materialized view",
  source: "Source",
  entity: "Catalog relation",
  unresolved: "Not found",
};

export function nodeKindLabel(kind: string) {
  return OBJECT_TYPE_LABELS[kind] ?? kind;
}

/** A stable color per group, by the group's place in the sorted group list. */
export function groupColor(
  group: string,
  allGroups: string[],
  colors: ThemeColors,
) {
  const index = Math.max(0, allGroups.indexOf(group));
  return colors.lineGraph[index % colors.lineGraph.length];
}
