// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { TextProps } from "@chakra-ui/react";

import { DatabaseObject } from "~/api/materialize/objects";
import { AllNamespaceItem as Schema } from "~/api/materialize/schemaList";

import { NULL_DATABASE_NAME } from "./constants";
import { ObjectExplorerParams } from "./routerHelpers";

export const OBJECT_TYPE_TO_LABEL = {
  connection: "Connections",
  index: "Indexes",
  "materialized-view": "Materialized Views",
  secret: "Secrets",
  sink: "Sinks",
  source: "Sources",
  table: "Tables",
  view: "Views",
} as const;

export type SupportedObjectType = keyof typeof OBJECT_TYPE_TO_LABEL;

export const SUPPORTED_OBJECT_TYPES = Object.keys(
  OBJECT_TYPE_TO_LABEL,
) as SupportedObjectType[];

export const SUPPORTED_SYSTEM_OBJECT_TYPES = [
  "index",
  "source",
  "table",
  "view",
] as SupportedObjectType[];

export type ObjectExplorerNodeType =
  | "database"
  | "schema"
  | "objectType"
  | "schemaGroup" // e.g. system catalog
  | SupportedObjectType;

export type NodeKey = string;

export interface ObjectExplorerNode {
  key: NodeKey;
  databaseName: string | null;
  schemaName?: string;
  label: string;
  href?: string;
  childNodeIndent?: string;
  children: Map<NodeKey, ObjectExplorerNode>;
  objectType?: string;
  sourceType?: string;
  parent?: ObjectExplorerNode;
  textProps?: TextProps;
  type: ObjectExplorerNodeType;
  isSelected?: (params: ObjectExplorerParams) => boolean;
  infoTooltip?: string;
}

export function databaseKey(obj: { databaseName: string | null }) {
  return obj.databaseName ?? NULL_DATABASE_NAME;
}

export function schemaKey(schema: Pick<Schema, "databaseName" | "schemaName">) {
  return `${schema.databaseName ?? NULL_DATABASE_NAME}/${schema.schemaName}`;
}

export function objectTypeKey(object: DatabaseObject): string;
export function objectTypeKey(schema: Schema, type: string): string;
export function objectTypeKey(
  objectOrSchema: DatabaseObject | Schema,
  type?: string,
) {
  if ("id" in objectOrSchema) {
    return `${objectOrSchema.databaseName ?? NULL_DATABASE_NAME}/${objectOrSchema.schemaName}/types/${objectOrSchema.objectType}`;
  }
  return `${objectOrSchema.databaseName ?? NULL_DATABASE_NAME}/${objectOrSchema.schemaName}/types/${type}`;
}

export function objectKey(
  object: Pick<DatabaseObject, "databaseName" | "schemaName" | "name">,
) {
  return `${object.databaseName ?? NULL_DATABASE_NAME}/${object.schemaName}/objects/${object.name}`;
}

export function selectedNodeKey({
  objectName,
  databaseName,
  schemaName,
}: ObjectExplorerParams) {
  if (objectName && databaseName && schemaName) {
    return objectKey({
      name: objectName,
      databaseName: databaseName ?? NULL_DATABASE_NAME,
      schemaName: schemaName,
    });
  }
  if (!objectName && schemaName) {
    return schemaKey({
      databaseName: databaseName ?? NULL_DATABASE_NAME,
      schemaName,
    });
  }
  if (!objectName && !schemaName) {
    return databaseKey({
      databaseName: databaseName ?? NULL_DATABASE_NAME,
    });
  }

  // In practice, this cases is only when no object is selected, and all the params are
  // undefined
  return undefined;
}

export function isParentOf(
  target: ObjectExplorerNode,
  possibleParent: ObjectExplorerNode,
) {
  let parent = target.parent;
  while (parent) {
    if (parent === possibleParent) {
      return true;
    }
    parent = parent.parent;
  }
  return false;
}

export function closestSelectableNodeHref(startingNode: ObjectExplorerNode) {
  let node: ObjectExplorerNode | undefined = startingNode;
  while (node) {
    if (node.href) {
      return node.href;
    }
    node = node.parent;
  }
}
