// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { DatabaseObject } from "~/api/materialize/objects";
import { AllNamespaceItem as Schema } from "~/api/materialize/schemaList";
import { MaterializeTheme } from "~/theme";

import { NULL_DATABASE_NAME } from "./constants";
import {
  databaseKey,
  NodeKey,
  OBJECT_TYPE_TO_LABEL,
  ObjectExplorerNode,
  objectKey,
  objectTypeKey,
  schemaKey,
  SUPPORTED_OBJECT_TYPES,
  SUPPORTED_SYSTEM_OBJECT_TYPES,
  SupportedObjectType,
} from "./ObjectExplorerNode";
import {
  ObjectExplorerParams,
  relativeDatabasePath,
  relativeObjectPath,
  relativeSchemaPath,
} from "./routerHelpers";

export function buildObjectExplorerTree(
  schemas: Schema[],
  objects: DatabaseObject[],
  selectObjectTypes: string[],
  colors: MaterializeTheme["colors"],
) {
  const databases = new Map<NodeKey, ObjectExplorerNode>();
  const systemCatalogNode: ObjectExplorerNode = {
    childNodeIndent: "0px",
    children: new Map(),
    databaseName: null,
    key: "systemCatalog",
    label: "System catalog",
    objectType: "schemaGroup",
    type: "schemaGroup" as SupportedObjectType,
    infoTooltip:
      "System catalog schemas contain metadata about your Materialize region.",
  };
  const allNodes = new Map<NodeKey, ObjectExplorerNode>();
  allNodes.set(systemCatalogNode.key, systemCatalogNode);
  for (const schema of schemas.sort((a, b) =>
    (a.schemaName ?? "").localeCompare(b.schemaName ?? ""),
  )) {
    if (schema.databaseId && schema.databaseName) {
      let databaseNode = databases.get(databaseKey(schema));
      if (!databaseNode) {
        databaseNode = {
          children: new Map(),
          databaseName: schema.databaseName,
          href: `../${relativeDatabasePath({ databaseName: schema.databaseName })}`,
          isSelected: isDatabaseSelected,
          key: databaseKey(schema),
          label: schema.databaseName,
          type: "database",
        };
        databases.set(databaseNode.key, databaseNode);
        allNodes.set(databaseNode.key, databaseNode);
      }
      if (schema.schemaName) {
        const schemaNode = addSchemaAndTypes(
          allNodes,
          colors,
          databaseNode,
          schema,
          selectObjectTypes,
        );
        databaseNode.children.set(schemaNode.key, schemaNode);
      }
    } else {
      const schemaNode = addSchemaAndTypes(
        allNodes,
        colors,
        undefined,
        schema,
        selectObjectTypes,
      );
      systemCatalogNode.children.set(schemaNode.key, schemaNode);
      allNodes.set(schemaNode.key, schemaNode);
    }
  }

  for (const object of objects.sort((a, b) => a.name.localeCompare(b.name))) {
    const parentNode = allNodes.get(objectTypeKey(object));
    if (!parentNode) continue;
    // progress sources aren't useful and will be removed
    if (object.sourceType === "progress") continue;

    const objectPath = relativeObjectPath({
      databaseName: object.databaseName,
      schemaName: object.schemaName,
      objectType: object.objectType,
      objectName: object.name,
      id: object.id,
    });
    const objectNode: ObjectExplorerNode = {
      childNodeIndent: "0px",
      children: new Map(),
      databaseName: object.databaseName,
      href: `../${objectPath}`,
      isSelected: isObjectSelected,
      key: objectKey(object),
      label: object.name,
      objectType: object.objectType,
      parent: parentNode,
      sourceType: object.sourceType ?? undefined,
      schemaName: object.schemaName,
      textProps: {
        color: colors.foreground.secondary,
      },
      type: object.objectType as SupportedObjectType,
    };
    parentNode.children.set(objectNode.key, objectNode);
    allNodes.set(objectNode.key, objectNode);
  }

  return {
    allNodes,
    databases,
    systemCatalogNode,
  };
}

function isDatabaseSelected(
  this: ObjectExplorerNode,
  params: ObjectExplorerParams,
) {
  return (
    !params.schemaName &&
    !params.objectName &&
    params.databaseName === this.databaseName
  );
}

function isSchemaSelected(
  this: ObjectExplorerNode,
  params: ObjectExplorerParams,
) {
  return (
    !params.objectName &&
    !params.objectType &&
    params.schemaName === this.schemaName &&
    params.databaseName === (this.databaseName ?? NULL_DATABASE_NAME)
  );
}

function isObjectSelected(
  this: ObjectExplorerNode,
  params: ObjectExplorerParams,
) {
  return (
    params.objectName === this.label &&
    params.schemaName === this.schemaName &&
    params.databaseName === (this.databaseName ?? NULL_DATABASE_NAME)
  );
}

function addSchemaAndTypes(
  allNodes: Map<NodeKey, ObjectExplorerNode>,
  colors: MaterializeTheme["colors"],
  parentNode: ObjectExplorerNode | undefined,
  schema: Schema,
  selectObjectTypes: string[],
) {
  const schemaPath = relativeSchemaPath({
    databaseName: schema.databaseName ?? NULL_DATABASE_NAME,
    schemaName: schema.schemaName ?? "",
  });
  const schemaNode: ObjectExplorerNode = {
    children: new Map(),
    databaseName: schema.databaseName ?? NULL_DATABASE_NAME,
    href: `../${schemaPath}`,
    isSelected: isSchemaSelected,
    key: schemaKey(schema),
    label: schema.schemaName ?? "",
    parent: parentNode,
    schemaName: schema.schemaName ?? "",
    type: "schema" as const,
  };
  allNodes.set(schemaNode.key, schemaNode);
  // if parentNode is not passed in, this is a system schema
  let types = parentNode
    ? SUPPORTED_OBJECT_TYPES
    : SUPPORTED_SYSTEM_OBJECT_TYPES;
  types =
    selectObjectTypes.length > 0
      ? types.filter((t) => selectObjectTypes.includes(t))
      : types;
  for (const type of types) {
    const objectTypeNode: ObjectExplorerNode = {
      children: new Map(),
      databaseName: schema.databaseName,
      key: objectTypeKey(schema, type),
      label: OBJECT_TYPE_TO_LABEL[type],
      objectType: type,
      parent: schemaNode,
      schemaName: schema.schemaName ?? "",
      textProps: {
        color: colors.foreground.secondary,
      },
      type: "objectType" as const,
    };
    schemaNode.children.set(objectTypeNode.key, objectTypeNode);
    allNodes.set(objectTypeNode.key, objectTypeNode);
  }

  return schemaNode;
}
