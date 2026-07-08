// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Button, HStack, Spinner, Text, VStack } from "@chakra-ui/react";
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

import { DataflowBreadcrumbs } from "./DataflowBreadcrumbs";
import {
  type Address,
  allSearchMatches,
  commonAncestorScope,
  decorateGraph,
  DEFAULT_FILTERS,
  deriveVisibleGraph,
  type Filters,
  type NodeId,
  nodeIdOf,
  type PortPeer,
  representativeInView,
  subtreeSearchMatches,
} from "./dataflowGraph";
import { DataflowGraphView } from "./DataflowGraphView";
import { DataflowToolbar } from "./DataflowToolbar";
import { LirPanel } from "./LirPanel";
import { NodeDetailPanel, type Selection } from "./NodeDetailPanel";
import { UsagePrivilegeAlert } from "./UsagePrivilegeAlert";

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

  const [rawFocusedScope, setFocusedScope] = React.useState<NodeId | null>(
    null,
  );
  // Derived, not synchronized via effect: a scope from a previous structure
  // (a stale drill-down target after a refetch or a dataflow/replica switch
  // changes the node id set) falls back to the new structure's root rather
  // than crashing every direct `nodes.get(focusedScope)!` lookup downstream.
  const focusedScope = data
    ? rawFocusedScope && data.structure.nodes.has(rawFocusedScope)
      ? rawFocusedScope
      : data.structure.root
    : null;
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

  // The route only remounts on a clusterId change, so switching dataflow or
  // replica in place (via the dropdowns) keeps this component instance, its
  // selection, filters, and pins alive. Those reference node ids from the
  // old structure. On the new one they silently miss instead of crashing
  // (see focusedScope above), which for a pin means every node reads as
  // "not in the highlighted set" and the whole graph dims with no way to
  // un-pin. Reset render-phase, not via effect, so there is no render in
  // between showing the new graph under the old pins.
  const resetKey = JSON.stringify([replicaName, dataflowId]);
  const [trackedResetKey, setTrackedResetKey] = React.useState(resetKey);
  if (resetKey !== trackedResetKey) {
    setTrackedResetKey(resetKey);
    setFocusedScope(null);
    setSelection(null);
    setFilters(DEFAULT_FILTERS);
    setMatchIndex(0);
    setLirHighlight(null);
    setPinnedLir(new Map());
  }

  const centerRef = React.useRef<((id: string) => void) | null>(null);
  const fitRef = React.useRef<((ids: string[]) => void) | null>(null);

  // Navigates to the scope that makes every given address directly visible
  // (as itself, or as the box that rolls it up), then fits the view to
  // whichever representative boxes result once that scope's layout lands.
  const navigateAndFit = React.useCallback(
    (addresses: Address[]) => {
      if (!data || addresses.length === 0) return;
      const scope = commonAncestorScope(data.structure, addresses);
      setFocusedScope(scope);
      const scopeAddress = data.structure.nodes.get(scope)!.address;
      const targets = [
        ...new Set(
          addresses
            .map((a) => representativeInView(a, scopeAddress))
            .filter((id): id is NodeId => id !== null),
        ),
      ];
      setPendingFitIds(targets);
    },
    [data],
  );

  const onLirSelect = React.useCallback(
    (memberIds: NodeId[]) => {
      if (!data) return;
      const addresses = memberIds
        .map((id) => data.structure.nodes.get(id)?.address)
        // The dataflow root is never a direct child of any scope, so it
        // can never be shown or highlighted; a LIR span that happens to
        // cover it simply can't anchor navigation on it.
        .filter((a): a is Address => a !== undefined && a.length > 1);
      navigateAndFit(addresses);
    },
    [data, navigateAndFit],
  );

  // Navigates to a scope and selects one specific box in it. Deriving the
  // destination's graph to find that box doesn't need to wait for anything
  // async (unlike fitting, which needs real layout positions from elk), so
  // this can select synchronously, right here, in the same call that
  // navigates — landing at a scope with many siblings (e.g. the root) never
  // leaves it ambiguous which one a jump actually reached.
  const focusOn = React.useCallback(
    (scope: NodeId, targetId: NodeId) => {
      if (!data) return;
      setFocusedScope(scope);
      setPendingFitIds([targetId]);
      const node = deriveVisibleGraph(data.structure, scope).nodes.find(
        (n) => n.id === targetId,
      );
      setSelection(node ? { kind: "node", node } : null);
    },
    [data],
  );

  // A port's peer lives outside the current view. When the peer is itself a
  // region, peerPortId names the exact port this crossing lands on from
  // inside it, so the jump drills straight there instead of parking outside
  // as an unlabeled box. A leaf peer has no inside to drill into, so the
  // fallback lands on the peer itself, in its own containing scope.
  const onJumpToPeer = React.useCallback(
    (peer: PortPeer) => {
      if (!data) return;
      if (peer.peerPortId) {
        focusOn(nodeIdOf(peer.address), peer.peerPortId);
        return;
      }
      const scope = commonAncestorScope(data.structure, [peer.address]);
      const scopeAddress = data.structure.nodes.get(scope)!.address;
      const targetId = representativeInView(peer.address, scopeAddress);
      if (targetId) focusOn(scope, targetId);
    },
    [data, focusOn],
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

  const allMatches = React.useMemo(
    () => (data ? allSearchMatches(data.structure, filters.search) : []),
    [data, filters.search],
  );
  // matchIndex resets to 0 only when the search text changes (see the effect
  // below). A refetch that shrinks the match set for the same search text,
  // or the render between a search-text change and that effect running,
  // otherwise leaves it pointing past the end of allMatches.
  const activeMatchIndex = Math.min(matchIndex, allMatches.length - 1);

  // Shared with DataflowGraphView via the visible prop below, so the current
  // scope's graph is derived once per render instead of once here and again
  // there.
  const visibleGraph = React.useMemo(
    () =>
      data && focusedScope
        ? deriveVisibleGraph(data.structure, focusedScope)
        : null,
    [data, focusedScope],
  );

  const decorations = React.useMemo(() => {
    if (!data || !focusedScope || !visibleGraph) return undefined;
    const visible = visibleGraph;
    const searchInfo = filters.search
      ? subtreeSearchMatches(data.structure, filters.search)
      : null;
    const d = decorateGraph(visible, filters, heatColor, searchInfo);
    // Pinned LIR ids dim everything outside the union of their members;
    // hovering a row (pinned or not) previews it the same way, on top of
    // whatever is already pinned. A member outside the current scope, or
    // rolled up into one of its boxes, highlights that box instead of being
    // silently dropped.
    const memberGroups = [
      ...(lirHighlight ? [[...lirHighlight]] : []),
      ...pinnedLir.values(),
    ];
    if (memberGroups.length > 0) {
      const focusedScopeAddress =
        data.structure.nodes.get(focusedScope)!.address;
      const highlighted = new Set<string>();
      for (const group of memberGroups) {
        for (const id of group) {
          const address = data.structure.nodes.get(id)?.address;
          const rep =
            address && representativeInView(address, focusedScopeAddress);
          if (rep) highlighted.add(rep);
        }
      }
      for (const n of visible.nodes) {
        if (!highlighted.has(n.id)) d.dimmedNodeIds.add(n.id);
      }
    }
    return d;
  }, [data, focusedScope, visibleGraph, filters, lirHighlight, pinnedLir]);

  React.useEffect(() => {
    setMatchIndex(0);
  }, [filters.search]);

  const onJump = React.useCallback(
    (delta: 1 | -1) => {
      if (allMatches.length === 0) return;
      const next =
        (activeMatchIndex + delta + allMatches.length) % allMatches.length;
      setMatchIndex(next);
      navigateAndFit([allMatches[next].address]);
    },
    [allMatches, activeMatchIndex, navigateAndFit],
  );
  // Digest of the sorted node ids: identical structure across a stats-only
  // refresh yields the same key, so layout and collapse state are preserved.
  // Memoized since sorting and hashing every node id is real work on a
  // dataflow with tens of thousands of operators, and this otherwise reran
  // on every render, not just on a new structure.
  const structureKey = React.useMemo(() => {
    if (!data) return null;
    const ids = [...data.structure.nodes.keys()].sort();
    return `${params?.dataflowId}/${params?.replicaName}/${ids.length}-${hashString(ids.join(","))}`;
  }, [data, params?.dataflowId, params?.replicaName]);
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
    // minH=0: MainContentContainer is a flex column item of BaseLayout's
    // <main>, which defaults to min-height:auto and so refuses to shrink
    // below its content's natural size. Harmless for pages that just scroll,
    // but this page pins height=100% end-to-end so the graph area can fill
    // whatever's left; without this override, one more row of content above
    // the graph (e.g. the breadcrumb trail) pushes the whole page taller
    // than the viewport instead of the graph area shrinking to absorb it.
    <MainContentContainer width="100%" minH={0}>
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
            flex="1"
            minWidth="320px"
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
          <UsagePrivilegeAlert action="visualize this dataflow" />
        ) : error ? (
          <VStack alignItems="flex-start" spacing={2}>
            <ErrorBox message="There was an error visualizing your dataflow" />
            <Button size="sm" onClick={refetch}>
              Retry
            </Button>
          </VStack>
        ) : !data || !focusedScope ? (
          <Spinner />
        ) : data.structure.nodes.size <= 1 ? (
          <Text>
            This dataflow no longer exists on this replica.{" "}
            <Link to="..">Back to dataflows</Link>
          </Text>
        ) : (
          <VStack flex="1" minH={0} alignItems="stretch" spacing={2}>
            <DataflowBreadcrumbs
              structure={data.structure}
              focusedScope={focusedScope}
              onNavigate={setFocusedScope}
            />
            <HStack flexShrink={0} flexWrap="wrap" alignItems="center">
              <DataflowToolbar
                filters={filters}
                onFiltersChange={setFilters}
                matchCount={allMatches.length}
                matchIndex={activeMatchIndex}
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
                // Non-null: this branch only renders once data and
                // focusedScope are both set, which is exactly when
                // visibleGraph is computed above.
                visible={visibleGraph!}
                focusedScope={focusedScope}
                onNavigate={setFocusedScope}
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
                activeMatchId={
                  allMatches.length > 0
                    ? nodeIdOf(allMatches[activeMatchIndex].address)
                    : undefined
                }
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
                onJumpToPeer={onJumpToPeer}
              />
              {selection && (
                <NodeDetailPanel
                  selection={selection}
                  onClose={() => setSelection(null)}
                  onJumpTo={onJumpToPeer}
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
