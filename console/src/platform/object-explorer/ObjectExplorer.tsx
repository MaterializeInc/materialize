// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  chakra,
  HStack,
  StackProps,
  Tooltip,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import debounce from "lodash.debounce";
import React, { useCallback, useEffect, useMemo, useRef } from "react";
import {
  useLocation,
  useNavigate,
  useParams,
  useSearchParams,
} from "react-router-dom";

import Alert from "~/components/Alert";
import { LoadingContainer } from "~/components/LoadingContainer";
import { SetNodeExpanded, Tree, TreeNode } from "~/components/Tree";
import { DraggableEvent, useDrag } from "~/hooks/useDrag";
import { useSyncObjectToSearchParams } from "~/hooks/useSyncObjectToSearchParams";
import { InfoIcon } from "~/icons";
import { useAllObjects } from "~/store/allObjects";
import { MaterializeTheme } from "~/theme";

import { useAllNamespaces } from "./allNamespaces";
import { Filters } from "./Filters";
import { objectIcon } from "./icons";
import {
  NodeKey,
  ObjectExplorerNode,
  selectedNodeKey,
} from "./ObjectExplorerNode";
import {
  buildUrlParamsObject,
  decodeUrlSearchParams,
  ObjectExplorerParams,
  objectExplorerReducer,
  ObjectTypeFilterUpdateFn,
} from "./objectExplorerState";

const OBJECT_EXPLORER_DEBOUNCE_MS = 250;

export const ObjectExplorerNav = (props: StackProps) => {
  const containerRef = useRef<HTMLDivElement | null>(null);

  const { colors } = useTheme<MaterializeTheme>();

  const onObjectExplorerNavResize = useCallback(
    (_: PointerEvent, draggableEvent: DraggableEvent) => {
      if (!containerRef.current) {
        return;
      }
      const curWidth = containerRef.current.getBoundingClientRect().width;

      const newWidth = curWidth + (draggableEvent.pointDelta?.x ?? 0);
      containerRef.current.style.width = `${newWidth}px`;
    },
    [],
  );

  const handleRef = useRef<HTMLButtonElement | null>(null);
  useDrag({
    ref: handleRef,
    onDrag: (...rest) => {
      onObjectExplorerNavResize(...rest);
    },
    onStart: () => {
      document.body.style.setProperty("cursor", "ew-resize");
    },
    onStop: () => {
      document.body.style.removeProperty("cursor");
    },
  });

  useEffect(() => {
    return () => {
      // In case this component unmounts before onStop is called
      document.body.style.removeProperty("cursor");
    };
  }, []);

  return (
    <HStack
      bg={colors.background.secondary}
      borderRightWidth={1}
      borderColor={colors.border.primary}
      alignItems="stretch"
      alignSelf="stretch"
      height="100%"
      flexShrink="0"
      minWidth="336px"
      maxWidth="600px"
      ref={containerRef}
      spacing="0"
      position="relative"
      px="2"
      py="6"
      width="336px"
    >
      <VStack width="100%" height="100%" spacing="2" alignItems="flex-start">
        {props.children}
      </VStack>
      <chakra.button
        height="100%"
        width="3"
        position="absolute"
        backgroundColor="transparent"
        zIndex={1}
        right="0"
        ref={handleRef}
        cursor="ew-resize"
      />
    </HStack>
  );
};

export const ObjectExplorer = () => {
  const [searchParams] = useSearchParams();
  const intitialFilters = decodeUrlSearchParams(searchParams);
  const {
    data: schemas,
    snapshotComplete: schemaSnapshotComplete,
    isError: isSchemaError,
  } = useAllNamespaces();
  const {
    data: objects,
    snapshotComplete: objectSnapshotComplete,
    isError: isObjectError,
  } = useAllObjects();
  const location = useLocation();
  const { colors } = useTheme<MaterializeTheme>();
  const params = useParams<ObjectExplorerParams>();
  const navigate = useNavigate();
  const [state, dispatch] = React.useReducer(objectExplorerReducer, {
    nameFilter: intitialFilters.name,
    objectTypeFilter: intitialFilters.objectType,
    expandedNodes: new Set<NodeKey>(),
    filteredObjectMap: new Map(),
    databases: [],
    colors,
    objects,
    schemas,
    navigateTo: undefined,
  });

  // We need to use a separate state variable for the search input because each keystroke
  // triggers a re-render of the tree, causing reflow issues. Thus per keystroke,
  // we call a debounced version of the dispatch to avoid this.
  const [searchInput, setSearchInput] = React.useState(state.nameFilter);
  const debouncedDispatch = useMemo(
    () => debounce(dispatch, OBJECT_EXPLORER_DEBOUNCE_MS),
    [dispatch],
  );

  const handleNameFilterChange = React.useCallback(
    (value: string) => {
      setSearchInput(value);
      debouncedDispatch({
        type: "updateState",
        payload: { nameFilter: value },
      });
    },
    [debouncedDispatch],
  );

  const selectedKey = React.useMemo(() => selectedNodeKey(params), [params]);
  const urlParamObject = React.useMemo(
    () => buildUrlParamsObject(state.nameFilter, state.objectTypeFilter),
    [state.nameFilter, state.objectTypeFilter],
  );
  useSyncObjectToSearchParams(urlParamObject);

  React.useEffect(() => {
    dispatch({ type: "updateState", payload: { schemas, objects, colors } });
  }, [colors, objects, schemas]);

  React.useEffect(() => {
    if (state.navigateTo) {
      navigate(
        { pathname: state.navigateTo, search: location.search },
        { state: { noExpand: true } },
      );
      dispatch({ type: "updateState", payload: { navigateTo: undefined } });
    }
  }, [location.search, navigate, state.navigateTo]);

  const toggleExpandNode = React.useCallback(
    (key: NodeKey) => {
      dispatch({
        type: "setNodeExpanded",
        key,
        selectedKey,
        setCallback: (prev) => !prev,
      });
    },
    [selectedKey],
  );

  React.useEffect(() => {
    if (!selectedKey) return;
    if (location.state?.noExpand) return;

    dispatch({
      type: "setNodeExpanded",
      key: selectedKey,
      selectedKey,
      setCallback: () => true,
    });
    // state.objects needs to be in this dependency array because on initial load, this
    // effect fires before the nodes are populated. One the tree is built, it will
    // re-run and expand the selected node.
  }, [state.objects, location.pathname, location.state, selectedKey]);

  return (
    <ObjectExplorerNav>
      <Filters
        nameFilter={searchInput}
        objectTypeFilter={state.objectTypeFilter}
        setNameFilter={handleNameFilterChange}
        setObjectTypeFilter={(updater: ObjectTypeFilterUpdateFn) => {
          dispatch({
            type: "updateObjectTypeFilter",
            updater,
          });
        }}
      />
      {isSchemaError || isObjectError ? (
        <Alert variant="error" message="An unexpected error has occurred" />
      ) : !schemaSnapshotComplete || !objectSnapshotComplete ? (
        <LoadingContainer />
      ) : (
        <VStack
          alignItems="flex-start"
          overflowY="auto"
          overflowX="clip"
          width="100%"
        >
          <Tree>
            {state.databases.map((child) => (
              <ObjectExplorerSubtree
                expandAllNodes={state.nameFilter.length > 0}
                expandedNodes={state.expandedNodes}
                node={child}
                key={child.key}
                toggleExpandNode={toggleExpandNode}
                setNodeExpanded={(key, setCallback) => {
                  dispatch({
                    type: "setNodeExpanded",
                    key,
                    selectedKey,
                    setCallback,
                  });
                }}
              />
            ))}
            {state.systemCatalogNode && (
              <ObjectExplorerSubtree
                expandAllNodes={state.nameFilter.length > 0}
                expandedNodes={state.expandedNodes}
                node={state.systemCatalogNode}
                key={state.systemCatalogNode.key}
                toggleExpandNode={toggleExpandNode}
                setNodeExpanded={(key, setCallback) => {
                  dispatch({
                    type: "setNodeExpanded",
                    key,
                    selectedKey,
                    setCallback,
                  });
                }}
              />
            )}
          </Tree>
        </VStack>
      )}
    </ObjectExplorerNav>
  );
};

export const ObjectExplorerSubtree = ({
  node,
  ...props
}: {
  expandedNodes: Set<NodeKey>;
  expandAllNodes: boolean;
  node: ObjectExplorerNode;
  toggleExpandNode: (key: NodeKey) => void;
  setNodeExpanded: SetNodeExpanded;
}) => {
  const params = useParams<ObjectExplorerParams>();

  return (
    <TreeNode
      childNodeIndent={node.childNodeIndent}
      data={Array.from(node.children.values())}
      href={node.href}
      icon={objectIcon(node.type, node.sourceType)}
      iconRight={
        node.infoTooltip && (
          <Tooltip label="System schemas contain metadata about your Materialize region.">
            <InfoIcon />
          </Tooltip>
        )
      }
      isExpanded={props.expandAllNodes || props.expandedNodes.has(node.key)}
      isSelected={node.isSelected?.(params) ?? false}
      label={node.label}
      nodeKey={node.key}
      setNodeExpanded={props.setNodeExpanded}
      textProps={node.textProps}
      toggleExpandNode={props.toggleExpandNode}
      key={node.key}
    >
      {(children) => (
        <Tree>
          {children.map((child) => (
            <ObjectExplorerSubtree {...props} key={child.key} node={child} />
          ))}
        </Tree>
      )}
    </TreeNode>
  );
};
