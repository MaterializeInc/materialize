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
  useNavigationType,
  useParams,
  useSearchParams,
} from "react-router-dom";

import { useDataflowGraphData } from "~/api/materialize/dataflow/useDataflowGraphData";
import { useDataflowList } from "~/api/materialize/dataflow/useDataflowList";
import { isInsufficientPrivilegeError } from "~/api/materialize/executeSql";
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
  lirIndex,
  lirSummary,
  mustGet,
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
import { hashString } from "./nodeStyle";
import { UsagePrivilegeAlert } from "./UsagePrivilegeAlert";

// Injected into decorateGraph so the pure graph module stays d3-free.
const heatColor = (t: number) => interpolateYlOrRd(0.15 + 0.85 * t);

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

  // The drill-down scope lives in the URL (as the dataflow address it
  // resolves to, e.g. "5.1.2"), not component state: back/forward then walk
  // the drill-down history, and a copied link reopens at the same scope.
  // Read fresh every render rather than synchronized via effect, so a scope
  // that doesn't resolve in the current structure (a stale link, a dataflow
  // whose shape changed, or simply no scope param) falls back to the root
  // rather than crashing every direct `nodes.get(focusedScope)!` lookup
  // downstream.
  const scopeParam = searchParams.get("scope");
  const rawFocusedScope = scopeParam
    ? nodeIdOf(scopeParam.split(".").map(Number))
    : null;
  const selectParam = searchParams.get("select");
  const focusedScope = data
    ? rawFocusedScope && data.structure.nodes.has(rawFocusedScope)
      ? rawFocusedScope
      : data.structure.root
    : null;
  // Every URL-param write funnels through here: calling setSearchParams
  // more than once in the same synchronous handler is unsafe (the second
  // call's `prev` doesn't see the first call's pending change, so it wins
  // and silently discards the first), so anything that needs to touch more
  // than one param at once (see focusOn, onSelectLir below) must do it in a
  // single mutate callback rather than composing two setters.
  const updateSearchParams = React.useCallback(
    (mutate: (urlParams: URLSearchParams) => void) => {
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev);
        mutate(next);
        return next;
      });
    },
    [setSearchParams],
  );
  // The root has no scope of its own (it's exactly a link's default), so
  // leave the URL as the plain dataflow link instead of carrying a
  // redundant, always-implied param.
  const applyScope = (urlParams: URLSearchParams, address: Address) => {
    if (address.length > 1) urlParams.set("scope", address.join("."));
    else urlParams.delete("scope");
  };
  const setFocusedScope = React.useCallback(
    (scope: NodeId) => {
      const address = data?.structure.nodes.get(scope)?.address;
      if (!address) return;
      // A selection from the old scope is almost never still valid in the
      // new one (a node's id generally isn't shared across scopes, and a
      // click immediately preceding a double-click-to-navigate already set
      // one for the region just navigated into, which won't be a listed
      // child of itself). focusOn and onSelectLir bypass this by writing
      // scope and selection together in their own updateSearchParams call,
      // so a real "navigate to X and select Y" still lands intact.
      updateSearchParams((urlParams) => {
        applyScope(urlParams, address);
        urlParams.delete("select");
      });
    },
    [data, updateSearchParams],
  );
  // Selection lives in the URL too (same reasoning as scope above): a
  // copied link, a reload, or back/forward all reopen at the same node,
  // edge, or LIR group, not just the same scope. Encoded as
  // `<kind>:<id>` in one param; kind is always one of the three literal
  // strings below, none of which contain ":", so splitting on the first
  // ":" recovers id intact even though ids themselves can (port and edge
  // ids do).
  const selectionParamValue = (next: Selection): string => {
    const id =
      next.kind === "node"
        ? next.node.id
        : next.kind === "edge"
          ? next.edge.id
          : next.node.key;
    return `${next.kind}:${id}`;
  };
  const setSelection = React.useCallback(
    (next: Selection | null) => {
      updateSearchParams((urlParams) => {
        if (next) urlParams.set("select", selectionParamValue(next));
        else urlParams.delete("select");
      });
    },
    [updateSearchParams],
  );
  const [filters, setFilters] = React.useState<Filters>(DEFAULT_FILTERS);
  const [matchIndex, setMatchIndex] = React.useState(0);
  const [lirHighlight, setLirHighlight] =
    React.useState<ReadonlySet<string> | null>(null);
  const [pendingFitIds, setPendingFitIds] = React.useState<string[] | null>(
    null,
  );
  // Persist independently of `selection`: closing (X) clears the selection
  // itself, so the next click naturally reopens the panel, but collapsing
  // should keep the panel out of the way across subsequent clicks too.
  const [lirPanelCollapsed, setLirPanelCollapsed] = React.useState(false);
  const [detailPanelCollapsed, setDetailPanelCollapsed] = React.useState(false);

  // The route only remounts on a clusterId change, so switching dataflow or
  // replica in place (via the dropdowns) keeps this component instance, its
  // filters alive. Selection and scope need no entry here: switching
  // dataflow or replica always lands on a fresh URL built from scratch (the
  // dropdowns pass only `replica`), which already carries neither a `scope`
  // nor a `select` param, so both already read back as their defaults (root,
  // nothing selected). Reset render-phase, not via effect, so there is no
  // render in between showing the new graph under the old filters. Calling
  // setSelection (a setSearchParams wrapper) here instead would be a
  // different matter: mutating router state mid-render, unlike this filters
  // state, which is local and always safe to update during render.
  const resetKey = JSON.stringify([replicaName, dataflowId]);
  const [trackedResetKey, setTrackedResetKey] = React.useState(resetKey);
  if (resetKey !== trackedResetKey) {
    setTrackedResetKey(resetKey);
    setFilters(DEFAULT_FILTERS);
    setMatchIndex(0);
    setLirHighlight(null);
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
      const scopeAddress = mustGet(data.structure.nodes, scope).address;
      const targets = [
        ...new Set(
          addresses
            .map((a) => representativeInView(a, scopeAddress))
            .filter((id): id is NodeId => id !== null),
        ),
      ];
      setPendingFitIds(targets);
    },
    [data, setFocusedScope],
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
      const address = data.structure.nodes.get(scope)?.address;
      if (!address) return;
      setPendingFitIds([targetId]);
      const node = deriveVisibleGraph(data.structure, scope).nodes.find(
        (n) => n.id === targetId,
      );
      updateSearchParams((urlParams) => {
        applyScope(urlParams, address);
        if (node) {
          urlParams.set("select", selectionParamValue({ kind: "node", node }));
        } else {
          urlParams.delete("select");
        }
      });
    },
    [data, updateSearchParams],
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
      const scopeAddress = mustGet(data.structure.nodes, scope).address;
      const targetId = representativeInView(peer.address, scopeAddress);
      if (targetId) focusOn(scope, targetId);
    },
    [data, focusOn],
  );

  const selectLirGroup = React.useCallback(
    (key: string) => {
      if (!data) return;
      const entry = lirIndex(data.structure).get(key);
      if (!entry) return;
      setSelection({
        kind: "lirGroup",
        node: {
          key,
          info: entry.info,
          memberIds: entry.memberIds,
          children: [],
          summary: lirSummary(data.structure, entry.memberIds),
        },
      });
    },
    [data, setSelection],
  );

  const onLirGroupClick = React.useCallback(
    (group: { id: string }) => selectLirGroup(group.id),
    [selectLirGroup],
  );

  // A node's own LIR entries link back to that LIR's group, wherever it
  // actually is: unlike clicking the group's box directly on the graph
  // (already visible, no navigation needed), the group here isn't
  // necessarily in view yet.
  const onSelectLir = React.useCallback(
    (exportId: string, lirId: string) => {
      if (!data) return;
      const key = `${exportId}/${lirId}`;
      const entry = lirIndex(data.structure).get(key);
      if (!entry) return;
      const addresses = entry.memberIds
        .map((id) => data.structure.nodes.get(id)?.address)
        .filter((a): a is Address => a !== undefined && a.length > 1);
      if (addresses.length === 0) return;
      // Inlines navigateAndFit's scope computation and selectLirGroup's
      // param value, rather than calling both, to land scope and
      // selection in one updateSearchParams call: two separate calls in
      // this same handler would have the second silently discard the
      // first (see updateSearchParams's doc comment above).
      const scope = commonAncestorScope(data.structure, addresses);
      const scopeAddress = mustGet(data.structure.nodes, scope).address;
      const targets = [
        ...new Set(
          addresses
            .map((a) => representativeInView(a, scopeAddress))
            .filter((id): id is NodeId => id !== null),
        ),
      ];
      setPendingFitIds(targets);
      const lirSelection: Selection = {
        kind: "lirGroup",
        node: {
          key,
          info: entry.info,
          memberIds: entry.memberIds,
          children: [],
          summary: lirSummary(data.structure, entry.memberIds),
        },
      };
      updateSearchParams((urlParams) => {
        applyScope(urlParams, scopeAddress);
        urlParams.set("select", selectionParamValue(lirSelection));
      });
    },
    [data, updateSearchParams],
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

  // Mirrors DataflowGraphView's own labelById: restoring a URL-driven
  // selection needs edge endpoint labels independently of a click event.
  const labelById = React.useMemo(
    () => new Map((visibleGraph?.nodes ?? []).map((n) => [n.id, n.label])),
    [visibleGraph],
  );

  // Re-derived from the current graph on every render rather than trusted
  // verbatim, the same tolerance focusedScope gets above: a stale link, or
  // a dataflow whose shape changed, just fails to resolve and this reads
  // back as null instead of crashing.
  const selection = React.useMemo<Selection | null>(() => {
    if (!data || !visibleGraph || !selectParam) return null;
    const sep = selectParam.indexOf(":");
    if (sep === -1) return null;
    const kind = selectParam.slice(0, sep);
    const id = selectParam.slice(sep + 1);
    if (kind === "node") {
      const node = visibleGraph.nodes.find((n) => n.id === id);
      if (!node) return null;
      const connectedEdges =
        node.kind === "port"
          ? visibleGraph.edges
              .filter((e) => e.source === node.id || e.target === node.id)
              .map((e) => ({
                ...e,
                sourceLabel: labelById.get(e.source) ?? e.source,
                targetLabel: labelById.get(e.target) ?? e.target,
              }))
          : undefined;
      return { kind: "node", node, connectedEdges };
    }
    if (kind === "edge") {
      const edge = visibleGraph.edges.find((e) => e.id === id);
      if (!edge) return null;
      return {
        kind: "edge",
        edge: {
          ...edge,
          sourceLabel: labelById.get(edge.source) ?? edge.source,
          targetLabel: labelById.get(edge.target) ?? edge.target,
        },
      };
    }
    if (kind === "lirGroup") {
      const entry = lirIndex(data.structure).get(id);
      if (!entry) return null;
      return {
        kind: "lirGroup",
        node: {
          key: id,
          info: entry.info,
          memberIds: entry.memberIds,
          children: [],
          summary: lirSummary(data.structure, entry.memberIds),
        },
      };
    }
    return null;
  }, [data, visibleGraph, selectParam, labelById]);

  // Back/forward (and opening a shared link directly, which react-router
  // also reports as "POP") can restore a selection the current viewport
  // isn't anywhere near — a click, by contrast, always selects something
  // already onscreen, so it doesn't need this. Re-fits only then, and only
  // for node/edge (a lirGroup's members can be scattered outside a single
  // fit's worth of space, so it's left to whatever the panel alone shows).
  const navigationType = useNavigationType();
  React.useEffect(() => {
    if (navigationType !== "POP" || !selection) return;
    if (selection.kind === "node") setPendingFitIds([selection.node.id]);
    else if (selection.kind === "edge") {
      setPendingFitIds([selection.edge.source, selection.edge.target]);
    }
  }, [navigationType, selection]);

  const decorations = React.useMemo(() => {
    if (!data || !focusedScope || !visibleGraph) return undefined;
    const visible = visibleGraph;
    const searchInfo = filters.search
      ? subtreeSearchMatches(data.structure, filters.search)
      : null;
    const d = decorateGraph(
      visible,
      filters,
      heatColor,
      searchInfo,
      data.workerCount,
    );
    // Hovering a LIR row in the sidebar dims everything outside its members.
    // A member outside the current scope, or rolled up into one of its
    // boxes, highlights that box instead of being silently dropped.
    if (lirHighlight) {
      const focusedScopeAddress = mustGet(
        data.structure.nodes,
        focusedScope,
      ).address;
      const highlighted = new Set<string>();
      for (const id of lirHighlight) {
        const address = data.structure.nodes.get(id)?.address;
        const rep =
          address && representativeInView(address, focusedScopeAddress);
        if (rep) highlighted.add(rep);
      }
      for (const n of visible.nodes) {
        if (!highlighted.has(n.id)) d.dimmedNodeIds.add(n.id);
      }
    }
    return d;
  }, [data, focusedScope, visibleGraph, filters, lirHighlight]);

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
  const permissionError = isInsufficientPrivilegeError(databaseError);

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
                workerCount={data.workerCount}
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
                collapsed={lirPanelCollapsed}
                onToggleCollapsed={() => setLirPanelCollapsed((prev) => !prev)}
                onHighlight={setLirHighlight}
                onSelect={onLirSelect}
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
                showLirGroups={filters.showLirGroups}
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
                onLirGroupClick={onLirGroupClick}
              />
              {selection && (
                <NodeDetailPanel
                  selection={selection}
                  collapsed={detailPanelCollapsed}
                  onToggleCollapsed={() =>
                    setDetailPanelCollapsed((prev) => !prev)
                  }
                  onClose={() => setSelection(null)}
                  onJumpTo={onJumpToPeer}
                  onSelectLir={onSelectLir}
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
