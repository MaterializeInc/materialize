// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Alert,
  AlertIcon,
  Box,
  Button,
  HStack,
  Spinner,
  Text,
  VStack,
} from "@chakra-ui/react";
import { interpolateYlOrRd } from "d3-scale-chromatic";
import React from "react";
import {
  Link,
  useNavigate,
  useParams,
  useSearchParams,
} from "react-router-dom";

import { useDataflowGraphData } from "~/api/materialize/dataflow/useDataflowGraphData";
import { useDataflowList } from "~/api/materialize/dataflow/useDataflowList";
import { ErrorCode } from "~/api/materialize/types";
import ErrorBox from "~/components/ErrorBox";
import LabeledSelect from "~/components/LabeledSelect";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { absoluteClusterPath } from "~/platform/routeHelpers";
import { useAllClusters } from "~/store/allClusters";
import { useRegionSlug } from "~/store/environments";

import {
  allChannelTypes,
  type CollapseState,
  decorateGraph,
  DEFAULT_FILTERS,
  defaultCollapseState,
  deriveVisibleGraph,
  expandAncestorsOf,
  expandForSearch,
  type Filters,
  type NodeId,
} from "./dataflowGraph";
import { DataflowGraphView } from "./DataflowGraphView";
import { DataflowToolbar } from "./DataflowToolbar";
import { LirPanel } from "./LirPanel";
import { NodeDetailPanel, type Selection } from "./NodeDetailPanel";

// Injected into decorateGraph so the pure graph module stays d3-free.
const heatColor = (t: number) => interpolateYlOrRd(0.15 + 0.85 * t);

// Stable 32-bit hash so a pure stats refresh (identical node ids) keeps the
// same structure key and reuses the layout, while any structural change
// produces a new key and relayouts.
function hashString(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++)
    h = (Math.imul(h, 31) + s.charCodeAt(i)) | 0;
  return h;
}

const DataflowDetailPage = () => {
  const { clusterId, dataflowId } = useParams();
  const navigate = useNavigate();
  const regionSlug = useRegionSlug();
  const { getClusterById } = useAllClusters();
  const cluster = clusterId ? getClusterById(clusterId) : undefined;
  const [searchParams, setSearchParams] = useSearchParams();
  const replicaName = searchParams.get("replica") ?? cluster?.replicas[0]?.name;

  const params = React.useMemo(
    () =>
      cluster && replicaName && dataflowId
        ? { clusterName: cluster.name, replicaName, dataflowId }
        : undefined,
    [cluster, replicaName, dataflowId],
  );
  const { data, error, databaseError, loading, refetch } =
    useDataflowGraphData(params);

  // Lets the toolbar switch dataflows in place instead of forcing a trip
  // back to the list page.
  const listParams = React.useMemo(
    () =>
      cluster && replicaName
        ? { clusterName: cluster.name, replicaName }
        : undefined,
    [cluster, replicaName],
  );
  const { data: dataflowList } = useDataflowList(listParams);

  const [collapsed, setCollapsed] = React.useState<CollapseState | null>(null);
  const [selection, setSelection] = React.useState<Selection | null>(null);
  const [filters, setFilters] = React.useState<Filters>(DEFAULT_FILTERS);
  const [matchIndex, setMatchIndex] = React.useState(0);
  const [lirHighlight, setLirHighlight] =
    React.useState<ReadonlySet<string> | null>(null);
  // Double-click pins a LIR id's dimming permanently (until toggled off
  // again), independent of hover, and more than one can be pinned at once.
  // Keeping memberIds alongside the key avoids re-deriving the LIR index here
  // just to resolve a key back to its members.
  const [pinnedLir, setPinnedLir] = React.useState<
    ReadonlyMap<string, NodeId[]>
  >(new Map());
  const [pendingFitIds, setPendingFitIds] = React.useState<string[] | null>(
    null,
  );
  const centerRef = React.useRef<((id: string) => void) | null>(null);
  const fitRef = React.useRef<((ids: string[]) => void) | null>(null);

  const onLirSelect = React.useCallback(
    (memberIds: NodeId[]) => {
      if (!data || memberIds.length === 0) return;
      const addresses = memberIds.map(
        (id) => data.structure.nodes.get(id)!.address,
      );
      setCollapsed((c) => (c ? expandAncestorsOf(c, addresses) : c));
      // Fitting happens once the expand above lands and the newly visible
      // members have real layout positions (see DataflowGraphView's
      // fitOnIds).
      setPendingFitIds(memberIds);
    },
    [data],
  );

  const onTogglePinLir = React.useCallback(
    (key: string, memberIds: NodeId[]) => {
      setPinnedLir((prev) => {
        const next = new Map(prev);
        if (next.has(key)) next.delete(key);
        else next.set(key, memberIds);
        return next;
      });
    },
    [],
  );

  const decorations = React.useMemo(() => {
    if (!data || !collapsed) return undefined;
    const visible = deriveVisibleGraph(data.structure, collapsed);
    const d = decorateGraph(visible, filters, heatColor);
    // Pinned LIR ids dim everything outside the union of their members;
    // hovering a row (pinned or not) previews it the same way, on top of
    // whatever is already pinned. Members collapsed away simply have no
    // visible node to keep lit.
    let highlighted: Set<string> | null = null;
    if (lirHighlight || pinnedLir.size > 0) {
      highlighted = new Set(lirHighlight ?? []);
      for (const memberIds of pinnedLir.values()) {
        for (const id of memberIds) highlighted.add(id);
      }
    }
    if (highlighted) {
      for (const n of visible.nodes) {
        if (!highlighted.has(n.id)) d.dimmedNodeIds.add(n.id);
      }
    }
    return d;
  }, [data, collapsed, filters, lirHighlight, pinnedLir]);

  // New search: expand ancestors of matches once, reset the cursor.
  React.useEffect(() => {
    setMatchIndex(0);
    if (filters.search && data) {
      setCollapsed((c) =>
        c ? expandForSearch(data.structure, c, filters.search) : c,
      );
    }
  }, [filters.search]); // eslint-disable-line react-hooks/exhaustive-deps

  const onJump = React.useCallback(
    (delta: 1 | -1) => {
      const matches = decorations?.searchMatches ?? [];
      if (matches.length === 0) return;
      const next = (matchIndex + delta + matches.length) % matches.length;
      setMatchIndex(next);
      centerRef.current?.(matches[next]);
    },
    [decorations?.searchMatches, matchIndex],
  );
  // Digest of the sorted node ids: identical structure across a stats-only
  // refresh yields the same key, so layout and collapse state are preserved.
  const structureKey = data
    ? (() => {
        const ids = [...data.structure.nodes.keys()].sort();
        return `${params?.dataflowId}/${params?.replicaName}/${ids.length}-${hashString(ids.join(","))}`;
      })()
    : null;
  React.useEffect(() => {
    if (data) setCollapsed(defaultCollapseState(data.structure));
    // Reset collapse state when the structure identity changes.
  }, [structureKey]); // eslint-disable-line react-hooks/exhaustive-deps

  if (!cluster) return null;
  if (cluster.replicas.length === 0) {
    return (
      <MainContentContainer width="100%">
        <Text>This cluster has no replicas.</Text>
      </MainContentContainer>
    );
  }
  const permissionError =
    databaseError &&
    "code" in databaseError &&
    databaseError.code === ErrorCode.INSUFFICIENT_PRIVILEGE;

  return (
    <MainContentContainer width="100%">
      <VStack width="100%" height="100%" alignItems="stretch">
        <HStack flexShrink={0} alignItems="flex-end">
          <LabeledSelect
            label="Replica"
            value={replicaName ?? ""}
            onChange={(e) => setSearchParams({ replica: e.target.value })}
            flexShrink={0}
          >
            {cluster.replicas.map((r) => (
              <option key={r.name} value={r.name}>
                {r.name}
              </option>
            ))}
          </LabeledSelect>
          <LabeledSelect
            label="Dataflow"
            value={dataflowId ?? ""}
            onChange={(e) =>
              navigate(
                `${absoluteClusterPath(regionSlug, cluster)}/dataflows/${e.target.value}?replica=${replicaName}`,
              )
            }
            flexShrink={0}
            width="320px"
          >
            {dataflowId && !dataflowList?.some((d) => d.id === dataflowId) && (
              // The list query hasn't resolved yet (or this dataflow just
              // disappeared from it); keep the select populated with
              // something so it isn't blank while data is in flight.
              <option value={dataflowId}>Loading…</option>
            )}
            {(dataflowList ?? []).map((d) => (
              <option key={d.id} value={d.id}>
                {d.name}
              </option>
            ))}
          </LabeledSelect>
        </HStack>
        {data && (
          <HStack flexShrink={0}>
            <Button size="sm" onClick={refetch} isDisabled={loading}>
              Refresh
            </Button>
            <Text fontSize="sm" color="foreground.secondary">
              Last fetched {data.fetchedAt.toLocaleTimeString()}
            </Text>
          </HStack>
        )}
        {permissionError ? (
          <Alert status="info" rounded="md" p={4} width="auto">
            <AlertIcon />
            <Text>
              You&apos;ll need{" "}
              <Text as="span" textStyle="monospace">
                USAGE
              </Text>{" "}
              privilege on this cluster to visualize this dataflow.
            </Text>
          </Alert>
        ) : error ? (
          <VStack alignItems="flex-start" spacing={2}>
            <ErrorBox message="There was an error visualizing your dataflow" />
            <Button size="sm" onClick={refetch}>
              Retry
            </Button>
          </VStack>
        ) : !data || !collapsed ? (
          <Spinner />
        ) : data.structure.nodes.size <= 1 ? (
          <Text>
            This dataflow no longer exists on this replica.{" "}
            <Link to="..">Back to dataflows</Link>
          </Text>
        ) : (
          <VStack flex="1" minH={0} alignItems="stretch" spacing={2}>
            <HStack flexShrink={0} flexWrap="wrap" alignItems="center">
              <DataflowToolbar
                filters={filters}
                onFiltersChange={setFilters}
                channelTypes={allChannelTypes(data.structure)}
                matchCount={decorations?.searchMatches.length ?? 0}
                matchIndex={matchIndex}
                onJump={onJump}
              />
              {filters.heatmap !== "off" && (
                <HStack spacing={1} flexShrink={0}>
                  <Text fontSize="xs" color="foreground.secondary">
                    low
                  </Text>
                  <Box
                    width="80px"
                    height="10px"
                    borderRadius="sm"
                    background={`linear-gradient(to right, ${heatColor(0)}, ${heatColor(0.5)}, ${heatColor(1)})`}
                  />
                  <Text fontSize="xs" color="foreground.secondary">
                    high
                  </Text>
                </HStack>
              )}
            </HStack>
            <HStack flex="1" minH={0} alignItems="stretch" spacing={0}>
              <LirPanel
                structure={data.structure}
                pinnedKeys={new Set(pinnedLir.keys())}
                onHighlight={setLirHighlight}
                onSelect={onLirSelect}
                onTogglePin={onTogglePinLir}
              />
              <DataflowGraphView
                structure={data.structure}
                collapsed={collapsed}
                onCollapsedChange={setCollapsed}
                cacheKey={structureKey ?? ""}
                decorations={decorations}
                centerRef={centerRef}
                fitRef={fitRef}
                fitOnIds={pendingFitIds}
                onFit={() => setPendingFitIds(null)}
                selectedId={
                  selection?.kind === "node"
                    ? selection.node.id
                    : selection?.kind === "edge"
                      ? selection.edge.id
                      : undefined
                }
                activeMatchId={decorations?.searchMatches[matchIndex]}
                onNodeClick={(node, connectedEdges) =>
                  setSelection({
                    kind: "node",
                    node,
                    connectedEdges:
                      node.kind === "port" ? connectedEdges : undefined,
                  })
                }
                onEdgeClick={(edge) => setSelection({ kind: "edge", edge })}
                onPaneClick={() => setSelection(null)}
              />
              {selection && (
                <NodeDetailPanel
                  selection={selection}
                  onClose={() => setSelection(null)}
                />
              )}
            </HStack>
          </VStack>
        )}
      </VStack>
    </MainContentContainer>
  );
};

export default DataflowDetailPage;
