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
import { decodeArraySearchParam } from "~/util";

import { buildObjectExplorerTree } from "./buildObjectExplorerTree";
import {
  closestSelectableNodeHref,
  isParentOf,
  NodeKey,
  ObjectExplorerNode,
  SUPPORTED_OBJECT_TYPES,
} from "./ObjectExplorerNode";

export type ObjectExplorerParams = {
  databaseName?: string;
  schemaName?: string;
  objectName?: string;
};

export interface ObjectExplorerState {
  nameFilter: string;
  objectTypeFilter: string[] | undefined;
  expandedNodes: Set<NodeKey>;
  filteredObjectMap: Map<NodeKey, ObjectExplorerNode>;
  databases: ObjectExplorerNode[];
  colors: MaterializeTheme["colors"];
  objects: DatabaseObject[];
  schemas: Schema[];
  systemCatalogNode?: ObjectExplorerNode;
  navigateTo: string | undefined;
}

export type ObjectTypeFilterUpdateFn = (
  previousFilter: string[] | undefined,
) => string[] | undefined;

export type UpdateObjectTypeFilterAction = {
  type: "updateObjectTypeFilter";
  updater: ObjectTypeFilterUpdateFn;
};

export type UpdateObjectExplorerStateAction = {
  type: "updateState";
  payload: Partial<ObjectExplorerState>;
};

export type SetNodeExpandedAction = {
  type: "setNodeExpanded";
  setCallback: (prev: boolean) => boolean;
  selectedKey?: NodeKey;
  key: NodeKey;
};

/**
 *
 * @param databases
 *  Separates databases with schemas and schema-less databases
 *  Alphabetically sorts each list
 *  Concatenates the two lists of databaseWithSchemas and databaseWithoutSchemas
 *
 */
function sortDatabasesWithEmptySchemas(
  databases: Map<NodeKey, ObjectExplorerNode>,
): Map<NodeKey, ObjectExplorerNode> {
  const allDatabaseNodes = Array.from(databases.values());
  const getLabel = (db: ObjectExplorerNode) => db.label?.toLowerCase() ?? "";

  const hasSchemaChild = (db: ObjectExplorerNode) =>
    db.children &&
    Array.from(db.children.values()).some((child) => child.type === "schema");

  const databasesWithSchemas = allDatabaseNodes
    .filter(hasSchemaChild)
    .sort((a, b) => getLabel(a).localeCompare(getLabel(b)));

  const databasesWithoutSchemas = allDatabaseNodes
    .filter((db) => !hasSchemaChild(db))
    .sort((a, b) => getLabel(a).localeCompare(getLabel(b)));

  const sortedDatabases = [...databasesWithSchemas, ...databasesWithoutSchemas];
  return new Map(sortedDatabases.map((db) => [db.key, db]));
}

export function objectExplorerReducer(
  state: ObjectExplorerState,
  action:
    | UpdateObjectTypeFilterAction
    | UpdateObjectExplorerStateAction
    | SetNodeExpandedAction,
): ObjectExplorerState {
  switch (action.type) {
    case "updateObjectTypeFilter": {
      const objectTypeFilter = action.updater(state.objectTypeFilter);
      return buildTreeWithFilters({
        ...state,
        objectTypeFilter,
      });
    }
    case "updateState": {
      return buildTreeWithFilters({
        ...state,
        ...action.payload,
      });
    }
    case "setNodeExpanded": {
      const newExpandedSet = new Set(state.expandedNodes.keys());
      const wasExpanded = newExpandedSet.has(action.key);
      const isExpanded = action.setCallback(wasExpanded);
      let navigateTo: string | undefined = undefined;

      if (wasExpanded) {
        // if we are collapsing a node, we need to check if the selected item is a
        // descendant of the collapsing node.
        const targetHref = findNearestVisibleNode(
          state,
          action.key,
          action.selectedKey,
        );
        // React doesn't allow updating state in multiple components at once, so we
        // record the new target path and navigate after the render.
        navigateTo = targetHref;
      }
      const node = state.filteredObjectMap.get(action.key);
      if (!node) return state;

      // Ensure all parents are expanded. This logic always executes because nodes can
      // be expanded, but in a collapsed subtree. By always executing this logic, we
      // ensure back / forward navigation properly expands the tree to show the
      // selected object.
      expandParentNodes(node, newExpandedSet);
      if (isExpanded) {
        newExpandedSet.add(action.key);
      } else {
        newExpandedSet.delete(action.key);
      }
      return buildTreeWithFilters({
        ...state,
        expandedNodes: newExpandedSet,
        navigateTo,
      });
    }
  }
}

export function buildUrlParamsObject(
  nameFilter: string,
  objectTypeFilter: string[] | undefined,
) {
  return {
    name: nameFilter,
    "object_type[]": objectTypeFilter,
  };
}

export function decodeUrlSearchParams(searchParams: URLSearchParams) {
  return {
    name: searchParams.get("name") ?? "",
    objectType: decodeArraySearchParam(
      searchParams,
      "object_type[]",
      undefined,
    ),
  };
}

function findNearestVisibleNode(
  state: ObjectExplorerState,
  key: NodeKey,
  selectedKey?: NodeKey,
) {
  if (!selectedKey) return;

  const collapsingNode = state.filteredObjectMap.get(key);
  const selectedNode = state.filteredObjectMap.get(selectedKey);
  if (
    !selectedNode ||
    !collapsingNode ||
    !isParentOf(selectedNode, collapsingNode)
  ) {
    return;
  }
  return closestSelectableNodeHref(collapsingNode);
}

function expandParentNodes(
  node: ObjectExplorerNode,
  expandedNodes: Set<NodeKey>,
) {
  let parent = node.parent;
  while (parent) {
    if (!expandedNodes.has(parent.key)) {
      expandedNodes.add(parent.key);
    }
    parent = parent.parent;
  }
}

const typesToRemove = ["database", "schema", "objectType"];

function removeEmptyNodes(nodes: Map<NodeKey, ObjectExplorerNode>) {
  for (const [key, node] of nodes.entries()) {
    if (!typesToRemove.includes(node.type)) continue;

    // first remove any children
    removeEmptyNodes(node.children);
    // now if there are no children left, remove the node
    if (node.children.size === 0) {
      nodes.delete(key);
    }
  }
}

function buildTreeWithFilters(state: ObjectExplorerState) {
  const filtered = state.objects.filter((o) =>
    o.name.includes(state.nameFilter),
  );

  const {
    allNodes: filteredObjectMap,
    databases,
    systemCatalogNode,
  } = buildObjectExplorerTree(
    state.schemas,
    filtered,
    state.objectTypeFilter ?? SUPPORTED_OBJECT_TYPES,
    state.colors,
  );
  if (state.nameFilter.length > 0) {
    // if there is a name filter, only show nodes with something in them
    removeEmptyNodes(databases);
    removeEmptyNodes(systemCatalogNode.children);
  }
  state.systemCatalogNode = systemCatalogNode;
  state.filteredObjectMap = filteredObjectMap;

  const sortedDatabases = sortDatabasesWithEmptySchemas(databases);
  state.databases = Array.from(sortedDatabases.values());
  return state;
}
