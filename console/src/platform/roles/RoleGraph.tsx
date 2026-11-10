// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Flex } from "@chakra-ui/react";
import React from "react";

import { RoleItem } from "~/api/materialize/roles/rolesList";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { Canvas, GraphEdge, GraphEdgeContainer } from "~/components/Graph";
import { LoadingContainer } from "~/components/LoadingContainer";
import {
  getDownstreamNodes,
  getUpstreamNodes,
} from "~/components/WorkflowGraph/graph";
import { useDagreGraph } from "~/hooks/useDagreGraph";
import { ROLE_GRAPH_CANVAS_Z_INDEX } from "~/layouts/zIndex";
import { notNullOrUndefined } from "~/util";

import { useRoleGraphEdges, useRolesList } from "./queries";
import {
  ROLE_NODE_HEIGHT,
  ROLE_NODE_WIDTH,
  RoleGraphNode,
} from "./RoleGraphNode";
import RoleGraphSidebar, { ROLE_SIDEBAR_WIDTH } from "./RoleGraphSidebar";

export const RoleGraph = () => {
  return (
    <AppErrorBoundary message="An error occurred fetching role data.">
      <React.Suspense fallback={<LoadingContainer />}>
        <RoleGraphInner />
      </React.Suspense>
    </AppErrorBoundary>
  );
};

const RoleGraphInner = () => {
  const [selectedRoleId, setSelectedRoleId] = React.useState<
    string | undefined
  >(undefined);
  const [filterValue, setFilterValue] = React.useState("");

  const { data: rolesData } = useRolesList();
  const roles = rolesData?.rows;

  const { data: edgesData } = useRoleGraphEdges();
  const edges = edgesData?.rows;

  // NOTE: Graph nodes are keyed by role id, never by name. graphlib keeps
  // nodes in plain objects, so a role named `__proto__` would otherwise write
  // to `Object.prototype` and break every later `fetch`.
  const nodes = React.useMemo(
    () =>
      roles?.map((role) => ({
        id: role.roleId,
        width: ROLE_NODE_WIDTH,
        height: ROLE_NODE_HEIGHT,
      })) ?? [],
    [roles],
  );

  const {
    graph,
    nodePositionMap,
    height,
    width,
    selectedNode,
    orderedGraphEdges,
    clampPoints,
  } = useDagreGraph({
    nodes,
    edges,
    selectedNodeId: selectedRoleId,
    ranksep: ROLE_NODE_HEIGHT * 2,
  });

  const roleMap = React.useMemo(() => {
    return new Map<string, RoleItem>(roles?.map((r) => [r.roleId, r]) ?? []);
  }, [roles]);

  const roleIdByName = React.useMemo(() => {
    return new Map<string, string>(
      roles?.map((r) => [r.roleName, r.roleId]) ?? [],
    );
  }, [roles]);

  const selectRoleByName = (roleName: string) => {
    setSelectedRoleId(roleIdByName.get(roleName));
  };

  const inheritsFrom = React.useMemo(() => {
    if (!selectedRoleId || !graph) return [];
    return getUpstreamNodes(selectedRoleId, graph)
      .map((id) => roleMap.get(id))
      .filter(notNullOrUndefined);
  }, [graph, roleMap, selectedRoleId]);

  const grantedTo = React.useMemo(() => {
    if (!selectedRoleId || !graph) return [];
    return getDownstreamNodes(selectedRoleId, graph)
      .map((id) => roleMap.get(id))
      .filter(notNullOrUndefined);
  }, [graph, roleMap, selectedRoleId]);

  const matchesFilter = (roleName: string) => {
    if (!filterValue) return true;
    return roleName.toLowerCase().includes(filterValue.toLowerCase());
  };

  const getFilterMatches = (value: string) => {
    if (!value || !roles) return [];
    return roles.filter((role) =>
      role.roleName.toLowerCase().includes(value.toLowerCase()),
    );
  };

  const handleFilterChange = (value: string) => {
    setFilterValue(value);
    if (!value) {
      // Don't clear selection when filter is cleared
      return;
    }
    const matches = getFilterMatches(value);
    if (matches.length === 1) {
      setSelectedRoleId(matches[0].roleId);
    } else {
      setSelectedRoleId(undefined);
    }
  };

  const handleRoleSelect = (roleName: string) => {
    selectRoleByName(roleName);
    setFilterValue("");
  };

  if (!roles || roles.length === 0 || !width || !height) {
    return <LoadingContainer />;
  }

  return (
    <Flex
      position="relative"
      minHeight="500px"
      height="calc(100vh - 300px)"
      flex="1"
    >
      <Canvas
        width={width}
        height={height}
        selectedNode={selectedNode}
        rightOffset={ROLE_SIDEBAR_WIDTH}
        controlsZIndex={ROLE_GRAPH_CANVAS_Z_INDEX}
      >
        {graph.nodes().map((roleId: string) => {
          const node = graph.node(roleId);
          const nodePosition = nodePositionMap.get(roleId);
          const role = roleMap.get(roleId);
          if (!node || !nodePosition || !role) return null;

          return (
            <RoleGraphNode
              key={roleId}
              roleData={role}
              isSelected={roleId === selectedRoleId}
              isFiltered={matchesFilter(role.roleName)}
              left={nodePosition.left}
              top={nodePosition.top}
              width={node.width}
              onClick={() => {
                setSelectedRoleId(roleId);
              }}
            />
          );
        })}
        <GraphEdgeContainer width={width} height={height}>
          {orderedGraphEdges.map((e) => {
            const edge = graph.edge(e);
            const parentNodePosition = nodePositionMap.get(e.v);
            if (!edge || !parentNodePosition) return null;

            const [firstPoint, ...points] = clampPoints(edge.points);

            return (
              <GraphEdge
                key={e.v + e.w}
                from={graph.node(e.v)}
                to={graph.node(e.w)}
                points={[
                  { x: firstPoint.x, y: parentNodePosition.bottom },
                  ...points,
                ]}
                isAdjacentToSelectedNode={
                  e.v === selectedRoleId || e.w === selectedRoleId
                }
              />
            );
          })}
        </GraphEdgeContainer>
      </Canvas>
      <RoleGraphSidebar
        selectedRole={selectedRoleId ? roleMap.get(selectedRoleId) : undefined}
        inheritsFrom={inheritsFrom}
        grantedTo={grantedTo}
        onRoleClick={selectRoleByName}
        filterValue={filterValue}
        onFilterChange={handleFilterChange}
        filteredRoles={getFilterMatches(filterValue)}
        onRoleSelect={handleRoleSelect}
      />
    </Flex>
  );
};

export default RoleGraph;
