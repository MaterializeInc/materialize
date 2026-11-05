// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
  Button,
  HStack,
  IconButton,
  Spinner,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useAtom, useSetAtom } from "jotai";
import debounce from "lodash.debounce";
import React, {
  Suspense,
  useEffect,
  useMemo,
  useReducer,
  useState,
} from "react";

import Alert from "~/components/Alert";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { LoadingContainer } from "~/components/LoadingContainer";
import { Tree, TreeNode } from "~/components/Tree";
import { PlayIcon, ReturnArrowIcon } from "~/icons";
import {
  useAllNamespacesLive,
  useSubscribeToAllNamespacesCollection,
} from "~/platform/object-explorer/allNamespacesCollection";
import { NULL_DATABASE_NAME } from "~/platform/object-explorer/constants";
import { Filters } from "~/platform/object-explorer/Filters";
import { objectIcon } from "~/platform/object-explorer/icons";
import { ObjectExplorerNav } from "~/platform/object-explorer/ObjectExplorer";
import {
  NodeKey,
  ObjectExplorerNode,
  SUPPORTED_OBJECT_TYPES,
} from "~/platform/object-explorer/ObjectExplorerNode";
import { objectExplorerReducer } from "~/platform/object-explorer/objectExplorerState";
import { useObjectColumns } from "~/platform/object-explorer/queries";
import {
  useAllObjectsLive,
  useSubscribeToAllObjectsCollection,
} from "~/store/allObjectsCollection";
import { MaterializeTheme } from "~/theme";

import { insertIntoActiveEditor } from "./activeEditor";
import { quoteIdentifier, shortestName } from "./sqlNames";
import { explorerQueryAtom, explorerWidthAtom } from "./store";

const NAME_FILTER_DEBOUNCE_MS = 250;
const QUERYABLE_TYPES = new Set([
  "table",
  "view",
  "materialized-view",
  "source",
]);
/** Object types that have columns to show. */
const RELATION_TYPES = new Set([
  "table",
  "view",
  "materialized-view",
  "source",
  "sink",
]);

interface SelectedObject {
  key: NodeKey;
  databaseName: string | null;
  schemaName: string;
  name: string;
  type: string;
  sourceType?: string;
}

/**
 * The Data explorer's tree, in the worksheet's sidebar. Clicking an object
 * shows its schema below the tree; double-clicking inserts its name into the
 * editor used last.
 */
export const WorksheetExplorer = () => {
  const setExplorerQuery = useSetAtom(explorerQueryAtom);
  const [width, setWidth] = useAtom(explorerWidthAtom);
  useSubscribeToAllObjectsCollection();
  useSubscribeToAllNamespacesCollection();
  const { colors } = useTheme<MaterializeTheme>();
  const namespaces = useAllNamespacesLive();
  const objects = useAllObjectsLive();
  const [selected, setSelected] = useState<SelectedObject>();
  const [state, dispatch] = useReducer(objectExplorerReducer, {
    nameFilter: "",
    objectTypeFilter: undefined,
    expandedNodes: new Set<NodeKey>(),
    filteredObjectMap: new Map(),
    databases: [],
    colors,
    objects: objects.data,
    schemas: namespaces.data,
    navigateTo: undefined,
  });
  const [nameInput, setNameInput] = useState("");
  const debouncedDispatch = useMemo(
    () => debounce(dispatch, NAME_FILTER_DEBOUNCE_MS),
    [],
  );
  useEffect(() => () => debouncedDispatch.cancel(), [debouncedDispatch]);

  useEffect(() => {
    dispatch({
      type: "updateState",
      payload: { schemas: namespaces.data, objects: objects.data, colors },
    });
  }, [namespaces.data, objects.data, colors]);

  // A selected object that was dropped leaves the schema panel.
  const selectedStillExists =
    !selected ||
    objects.data.some(
      (o) =>
        o.name === selected.name &&
        o.schemaName === selected.schemaName &&
        o.databaseName === selected.databaseName,
    );
  const shownSelection = selectedStillExists ? selected : undefined;

  const toggle = (key: NodeKey) =>
    dispatch({ type: "setNodeExpanded", key, setCallback: (prev) => !prev });

  const roots = [
    ...state.databases,
    ...(state.systemCatalogNode ? [state.systemCatalogNode] : []),
  ];

  return (
    <ObjectExplorerNav
      initialWidth={width}
      minResizeWidth={240}
      onResizeEnd={setWidth}
    >
      <Filters
        nameFilter={nameInput}
        objectTypeFilter={state.objectTypeFilter}
        setNameFilter={(value) => {
          setNameInput(value);
          debouncedDispatch({
            type: "updateState",
            payload: { nameFilter: value },
          });
        }}
        setObjectTypeFilter={(updater) =>
          dispatch({ type: "updateObjectTypeFilter", updater })
        }
      />
      {namespaces.isError || objects.isError ? (
        <Alert variant="error" message="An unexpected error has occurred" />
      ) : !namespaces.snapshotComplete || !objects.snapshotComplete ? (
        <LoadingContainer />
      ) : (
        <VStack
          alignItems="flex-start"
          overflowY="auto"
          overflowX="clip"
          width="100%"
          flex="1"
          minH="0"
        >
          <Tree>
            {roots.map((node) => (
              <ExplorerSubtree
                key={node.key}
                node={node}
                expandAll={state.nameFilter.length > 0}
                expandedNodes={state.expandedNodes}
                selectedKey={shownSelection?.key}
                onToggle={toggle}
                onSelect={setSelected}
                onQuery={setExplorerQuery}
              />
            ))}
          </Tree>
        </VStack>
      )}
      <Box
        width="100%"
        flexShrink={0}
        // A fixed share of the sidebar, whatever the selection, so the
        // tree above never jumps. The column list scrolls within it.
        height="40%"
        overflow="hidden"
        display="flex"
        flexDirection="column"
        borderTopWidth="1px"
        borderColor={colors.border.primary}
        pt="2"
      >
        {shownSelection ? (
          <AppErrorBoundary message="This object's schema could not be loaded">
            <Suspense
              fallback={
                <HStack px="2" py="2">
                  <Spinner size="xs" />
                </HStack>
              }
            >
              <SchemaPanel
                key={shownSelection.key}
                object={shownSelection}
                onQuery={setExplorerQuery}
              />
            </Suspense>
          </AppErrorBoundary>
        ) : (
          <Text
            textStyle="text-small"
            color={colors.foreground.tertiary}
            px="2"
          >
            Select a table, view or source to see its columns. Double-click a
            name to insert it into the editor.
          </Text>
        )}
      </Box>
    </ObjectExplorerNav>
  );
};

interface ExplorerSubtreeProps {
  node: ObjectExplorerNode;
  expandAll: boolean;
  expandedNodes: Set<NodeKey>;
  selectedKey: NodeKey | undefined;
  onToggle: (key: NodeKey) => void;
  onSelect: (object: SelectedObject) => void;
  onQuery: (sql: string) => void;
}

const ExplorerSubtree = ({ node, ...props }: ExplorerSubtreeProps) => {
  const isObject = (SUPPORTED_OBJECT_TYPES as string[]).includes(node.type);
  const name =
    isObject && node.schemaName
      ? shortestName(node.databaseName, node.schemaName, node.label)
      : node.type === "schema" || node.type === "database"
        ? quoteIdentifier(node.label)
        : undefined;
  const isExpanded = props.expandAll || props.expandedNodes.has(node.key);

  return (
    <TreeNode<ObjectExplorerNode>
      nodeKey={node.key}
      label={node.label}
      title={
        name ? `${node.label}. Double-click to insert ${name}.` : node.label
      }
      icon={objectIcon(node.type, node.sourceType)}
      childNodeIndent={node.childNodeIndent}
      data={Array.from(node.children.values())}
      isExpanded={isExpanded}
      isSelected={props.selectedKey === node.key}
      textProps={node.textProps}
      toggleExpandNode={props.onToggle}
      setNodeExpanded={() => {}}
      onSelect={
        isObject && node.schemaName
          ? () =>
              props.onSelect({
                key: node.key,
                databaseName: node.databaseName,
                schemaName: node.schemaName ?? "",
                name: node.label,
                type: node.type,
                sourceType: node.sourceType,
              })
          : () => props.onToggle(node.key)
      }
      onDoubleClick={name ? () => insertIntoActiveEditor(name) : undefined}
      actions={
        name && isObject ? (
          <>
            <IconButton
              aria-label={`Insert ${node.label}`}
              title={`Insert ${name}`}
              icon={<ReturnArrowIcon />}
              size="xs"
              variant="inline"
              minW="5"
              onClick={() => insertIntoActiveEditor(name)}
            />
            {QUERYABLE_TYPES.has(node.type) && (
              <IconButton
                aria-label={`Query ${node.label}`}
                title={`Add SELECT * FROM ${name} LIMIT 100 as a cell and run it`}
                icon={<PlayIcon />}
                size="xs"
                variant="inline"
                minW="5"
                onClick={() => props.onQuery(`SELECT * FROM ${name} LIMIT 100`)}
              />
            )}
          </>
        ) : undefined
      }
    >
      {(children) => (
        <Tree>
          {children.map((child) => (
            <ExplorerSubtree key={child.key} node={child} {...props} />
          ))}
        </Tree>
      )}
    </TreeNode>
  );
};

interface SchemaPanelProps {
  object: SelectedObject;
  onQuery: (sql: string) => void;
}

/** The selected object's columns, with their types. */
const SchemaPanel = ({ object, onQuery }: SchemaPanelProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const name = shortestName(
    object.databaseName,
    object.schemaName,
    object.name,
  );
  const hasColumns = RELATION_TYPES.has(object.type);
  const typeLabel = object.type.replace(/-/g, " ");
  const fullName = [object.databaseName, object.schemaName, object.name]
    .filter(Boolean)
    .join(".");

  return (
    <VStack alignItems="stretch" spacing="1.5" minH="0" flex="1" px="2">
      <HStack justifyContent="space-between" spacing="2">
        <HStack spacing="2" minW="0">
          {objectIcon(
            object.type as ObjectExplorerNode["type"],
            object.sourceType,
          )}
          <Text
            textStyle="text-small-heavy"
            fontFamily="mono"
            noOfLines={1}
            title={fullName}
          >
            {fullName}
          </Text>
        </HStack>
        <Text
          textStyle="text-small"
          color={colors.foreground.tertiary}
          flexShrink={0}
        >
          {typeLabel}
        </Text>
      </HStack>
      <HStack spacing="1">
        <Button
          size="xs"
          variant="secondary"
          onClick={() => insertIntoActiveEditor(name)}
        >
          Insert name
        </Button>
        {QUERYABLE_TYPES.has(object.type) && (
          <Button
            size="xs"
            variant="secondary"
            onClick={() => onQuery(`SELECT * FROM ${name} LIMIT 100`)}
          >
            Query
          </Button>
        )}
      </HStack>
      {hasColumns ? (
        <ColumnsTable object={object} />
      ) : (
        <Text textStyle="text-small" color={colors.foreground.tertiary}>
          {/^[aeiou]/.test(typeLabel) ? "An" : "A"} {typeLabel} has no columns.
        </Text>
      )}
    </VStack>
  );
};

const ColumnsTable = ({ object }: { object: SelectedObject }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data } = useObjectColumns({
    databaseName: object.databaseName ?? NULL_DATABASE_NAME,
    schemaName: object.schemaName,
    name: object.name,
  });
  const columns = data.rows;
  const comment = columns[0]?.relationComment;

  return (
    <Box overflowY="auto" minH="0" flex="1">
      {comment && (
        <Text textStyle="text-small" color={colors.foreground.secondary} mb="1">
          {comment}
        </Text>
      )}
      <Table
        size="sm"
        variant="shell"
        width="100%"
        sx={{
          "th, td": { px: 1, py: 0.5, fontSize: "xs", borderBottomWidth: 0 },
          td: { fontFamily: "mono" },
          th: { position: "sticky", top: 0, zIndex: 1 },
        }}
      >
        <Thead>
          <Tr>
            <Th>column</Th>
            <Th>type</Th>
          </Tr>
        </Thead>
        <Tbody>
          {columns.map((column) => (
            <Tr key={column.name}>
              <Td
                cursor="pointer"
                title={
                  column.columnComment
                    ? `${column.columnComment}\nClick to insert`
                    : "Click to insert"
                }
                _hover={{ color: colors.accent.purple }}
                onClick={() =>
                  insertIntoActiveEditor(quoteIdentifier(column.name))
                }
              >
                {column.name}
              </Td>
              <Td color={colors.foreground.secondary}>
                {column.type}
                {column.nullable ? "" : " not null"}
              </Td>
            </Tr>
          ))}
        </Tbody>
      </Table>
    </Box>
  );
};
