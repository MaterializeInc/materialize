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
  Portal,
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import {
  Link,
  type NavigateOptions,
  useNavigate,
  useNavigationType,
  useParams,
  useSearchParams,
} from "react-router-dom";

import { useDataflowGraphData } from "~/api/materialize/dataflow/useDataflowGraphData";
import { isResolvableExportId } from "~/api/materialize/dataflow/useDataflowIdForExport";
import { useDataflowList } from "~/api/materialize/dataflow/useDataflowList";
import { isInsufficientPrivilegeError } from "~/api/materialize/executeSql";
import ErrorBox from "~/components/ErrorBox";
import LabeledSelect from "~/components/LabeledSelect";
import { MaximizeIcon, MinimizeIcon } from "~/icons";
import { MainContentContainer } from "~/layouts/BaseLayout";
import { replicaSearch } from "~/platform/clusters/ClusterPage/tabLinks";
import { useSelectedReplica } from "~/platform/clusters/ClusterPage/useSelectedReplica";
import { absoluteClusterPath } from "~/platform/routeHelpers";
import { useAllClusters } from "~/store/allClusters";
import { useRegionSlug } from "~/store/environments";
import { MaterializeTheme } from "~/theme";

import { COLLAPSED_STRIP_WIDTH_PX } from "./CollapsedPanelStrip";
import { DataflowBreadcrumbs } from "./DataflowBreadcrumbs";
import {
  type Address,
  allSearchMatches,
  commonAncestorScope,
  dataflowExportIds,
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
import {
  DETAIL_PANEL_WIDTH_PX,
  NodeDetailPanel,
  type Selection,
} from "./NodeDetailPanel";
import { hashString, heatColorFor } from "./nodeStyle";
import { UsagePrivilegeAlert } from "./UsagePrivilegeAlert";

// The app layout's content column is a flex item with a z-index, which makes
// it a stacking context below the app nav, so nothing inside it can be drawn
// over the nav at any z-index. Portaling out of it remounts the children on
// every toggle, which costs one relayout.
const MaybePortal = ({
  enabled,
  children,
}: {
  enabled: boolean;
  children: React.ReactNode;
}) => (enabled ? <Portal>{children}</Portal> : <>{children}</>);

const DataflowDetailPage = () => {
  const { colorMode } = useTheme<MaterializeTheme>();
  // Injected into decorateGraph so the pure graph module stays d3-free.
  const heatColor = React.useMemo(() => heatColorFor(colorMode), [colorMode]);
  const { clusterId, dataflowId } = useParams();
  const navigate = useNavigate();
  const regionSlug = useRegionSlug();
  const { getClusterById } = useAllClusters();
  const cluster = clusterId ? getClusterById(clusterId) : undefined;
  const [searchParams, setSearchParams] = useSearchParams();
  // A `?replica=` naming no current replica (a stale link, a dropped
  // replica) falls back to the first replica rather than reaching SQL.
  const { selectedReplica } = useSelectedReplica(cluster);
  const replicaName = selectedReplica?.name ?? cluster?.replicas[0]?.name;

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
  // The regions expanded in place, same URL-lives-the-state reasoning as
  // scope and selection above. Re-derived (not trusted verbatim) for the
  // same tolerance: an id from a stale link, a data refresh that removed a
  // region, or a link bookmarked in a different scope, is dropped rather
  // than fed downstream, since deriveVisibleGraph and representativeInView
  // both assume every entry names a real region inside the current view.
  const expandParam = searchParams.get("expand");
  const expandedScopes = React.useMemo(() => {
    const set = new Set<NodeId>();
    if (!data || !focusedScope) return set;
    const focusedAddress = data.structure.nodes.get(focusedScope)?.address;
    if (!focusedAddress) return set;
    for (const token of expandParam ? expandParam.split(",") : []) {
      const id = nodeIdOf(token.split(".").map(Number));
      const node = data.structure.nodes.get(id);
      // Keep only real regions inside the current view, dropping stale ids
      // left after a data refresh or a navigate to a different scope.
      if (
        node &&
        node.children.length > 0 &&
        node.address.length > focusedAddress.length &&
        focusedAddress.every((v, i) => node.address[i] === v)
      ) {
        set.add(id);
      }
    }
    return set;
  }, [data, focusedScope, expandParam]);
  // Every URL-param write funnels through here: calling setSearchParams
  // more than once in the same synchronous handler is unsafe (the second
  // call's `prev` doesn't see the first call's pending change, so it wins
  // and silently discards the first), so anything that needs to touch more
  // than one param at once (see focusOn, onSelectLir below) must do it in a
  // single mutate callback rather than composing two setters. `options`
  // passes straight through to setSearchParams (e.g. onToggleExpand below
  // passes `{ replace: true }`, since a scope navigation is worth a back
  // button stop but an expand/collapse click isn't).
  const updateSearchParams = React.useCallback(
    (
      mutate: (urlParams: URLSearchParams) => void,
      options?: NavigateOptions,
    ) => {
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev);
        mutate(next);
        return next;
      }, options);
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
      // so a real "navigate to X and select Y" still lands intact. Expanded
      // regions don't carry over either: expand's own subtree filter (see
      // expandedScopes above) would drop most of them anyway once the scope
      // moves, so clearing here just makes that explicit instead of leaving
      // whatever survives to chance.
      updateSearchParams((urlParams) => {
        applyScope(urlParams, address);
        urlParams.delete("select");
        urlParams.delete("expand");
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
  const [lirPanelCollapsed, setLirPanelCollapsed] = React.useState(false);
  // Collapsing applies to the selection it was done for. Any different
  // selection reopens the panel, including returning to an earlier one, so
  // the collapse is dropped render-phase (like resetKey below) as soon as
  // `select` moves, whichever handler or navigation moved it.
  const [collapsedForSelect, setCollapsedForSelect] = React.useState<
    string | null
  >(null);
  if (collapsedForSelect !== null && collapsedForSelect !== selectParam) {
    setCollapsedForSelect(null);
  }
  const detailPanelCollapsed =
    selectParam !== null && collapsedForSelect === selectParam;

  // Toggling doesn't move the focused scope, so it's worth a lot less as a
  // back-button stop than an actual navigation: replace rather than push, so
  // clicking a few regions open and shut doesn't bury the real navigation
  // history under them.
  const onToggleExpand = React.useCallback(
    (id: NodeId) => {
      if (!data) return;
      const address = data.structure.nodes.get(id)?.address;
      if (!address) return;
      const next = new Set(expandedScopes);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      updateSearchParams(
        (urlParams) => {
          const tokens = [...next].map((scopeId) =>
            mustGet(data.structure.nodes, scopeId).address.join("."),
          );
          if (tokens.length > 0) urlParams.set("expand", tokens.join(","));
          else urlParams.delete("expand");
        },
        { replace: true },
      );
      // Nothing else refits on a toggle (ViewportGuard only fires on a
      // focusedScope change), so an expansion that grows off-screen would
      // otherwise stay off-screen. Fit to the region and its now-visible
      // children once the new layout lands; a collapse needs no such fit,
      // since it can only shrink what's already onscreen.
      if (!next.has(id)) return;
      const childIds = mustGet(data.structure.nodes, id).children;
      setPendingFitIds([id, ...childIds]);
    },
    [data, expandedScopes, updateSearchParams],
  );

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

  // Walks every node in the structure, so it is built once per structure
  // rather than per selection: three handlers below resolve a LIR key
  // through it.
  const lirEntries = React.useMemo(
    () => (data ? lirIndex(data.structure) : null),
    [data],
  );

  const selectLirGroup = React.useCallback(
    (key: string) => {
      if (!data || !lirEntries) return;
      const entry = lirEntries.get(key);
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
    [data, lirEntries, setSelection],
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
      if (!data || !lirEntries) return;
      const key = `${exportId}/${lirId}`;
      const entry = lirEntries.get(key);
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
    [data, lirEntries, updateSearchParams],
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
        ? deriveVisibleGraph(data.structure, focusedScope, expandedScopes)
        : null,
    [data, focusedScope, expandedScopes],
  );

  // Mirrors DataflowGraphView's own labelById: restoring a URL-driven
  // selection needs edge endpoint labels independently of a click event.
  const labelById = React.useMemo(
    () => new Map((visibleGraph?.nodes ?? []).map((n) => [n.id, n.label])),
    [visibleGraph],
  );

  // Shared by the URL-driven `selection` below and by onSelectNode (a
  // click on an in-view edge's Source/Target link): both need the same
  // connectedEdges attached for a port, so it can still open the panel's
  // port view rather than the generic node view.
  const resolveNodeSelection = React.useCallback(
    (id: string): Selection | null => {
      if (!visibleGraph) return null;
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
    },
    [visibleGraph, labelById],
  );

  const onSelectNode = React.useCallback(
    (id: string) => {
      const next = resolveNodeSelection(id);
      if (next) setSelection(next);
    },
    [resolveNodeSelection, setSelection],
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
      return resolveNodeSelection(id);
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
      const entry = lirEntries?.get(id);
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
  }, [
    data,
    visibleGraph,
    selectParam,
    labelById,
    lirEntries,
    resolveNodeSelection,
  ]);

  // Back/forward (and opening a shared link directly, which react-router
  // also reports as "POP") can restore a selection the current viewport
  // isn't anywhere near — a click, by contrast, always selects something
  // already onscreen, so it doesn't need this. Re-fits only then, and only
  // for node/edge (a lirGroup's members can be scattered outside a single
  // fit's worth of space, so it's left to whatever the panel alone shows).
  //
  // `selection` gets a new identity on every refetch, so the effect keys on
  // the `select` param it was fitted for instead: a Refresh must not yank the
  // viewport back to a selection the user has since panned away from. The
  // marker resets whenever the last navigation was not a POP or nothing is
  // selected, so returning to an earlier selection by back/forward fits again.
  const navigationType = useNavigationType();
  const fittedSelectParam = React.useRef<string | null>(null);
  React.useEffect(() => {
    if (navigationType !== "POP" || !selection) {
      fittedSelectParam.current = null;
      return;
    }
    if (fittedSelectParam.current === selectParam) return;
    fittedSelectParam.current = selectParam;
    if (selection.kind === "node") setPendingFitIds([selection.node.id]);
    else if (selection.kind === "edge") {
      setPendingFitIds([selection.edge.source, selection.edge.target]);
    }
  }, [navigationType, selection, selectParam]);

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
          address &&
          representativeInView(address, focusedScopeAddress, expandedScopes);
        if (rep) highlighted.add(rep);
      }
      for (const n of visible.nodes) {
        if (!highlighted.has(n.id)) d.dimmedNodeIds.add(n.id);
      }
    }
    return d;
  }, [
    data,
    focusedScope,
    visibleGraph,
    filters,
    heatColor,
    lirHighlight,
    expandedScopes,
  ]);

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
  // Maximizing pins the visualizer root over the whole window, hiding the
  // app nav, cluster header and tabs. NOTE: the browser Fullscreen API would
  // hide Chakra tooltips, because they portal into document.body, which is
  // outside a fullscreen element. The root also has to leave the app layout
  // through a portal (see MaybePortal).
  const [isMaximized, setIsMaximized] = React.useState(false);
  React.useEffect(() => {
    if (!isMaximized) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") setIsMaximized(false);
    };
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, [isMaximized]);

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
  // Dataflow ids come from a per-replica counter, so the same id on another
  // replica names an unrelated dataflow. Rather than keep the id across a
  // replica switch, hand the list page this dataflow's export id to
  // re-resolve on the new replica. A dataflow with no known export (no LIR
  // data, or only ids the resolver rejects) goes to the new replica's plain
  // list.
  const onReplicaChange = (newReplicaName: string) => {
    if (!cluster) return;
    const listPath = `${absoluteClusterPath(regionSlug, cluster)}/dataflows`;
    const exportId = data
      ? dataflowExportIds(data.structure).find(isResolvableExportId)
      : undefined;
    const search = new URLSearchParams({ replica: newReplicaName });
    if (exportId !== undefined) search.set("export", exportId);
    navigate(`${listPath}?${search}`);
  };
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
      <MaybePortal enabled={isMaximized}>
        <VStack
          width="100%"
          height="100%"
          alignItems="stretch"
          // Below the "tooltip" and "popover" z-index tokens, so tooltips and
          // menus still render above the maximized page. The element is
          // transparent by default, so it needs the page background and the
          // inset the app layout otherwise supplies.
          {...(isMaximized && {
            position: "fixed",
            inset: 0,
            zIndex: "overlay",
            background: "background.primary",
            p: 4,
          })}
        >
          {/* One wrapping row: the graph canvas is the point of this page, so
            every row above it costs usable height. */}
          <HStack
            flexShrink={0}
            alignItems="flex-end"
            flexWrap="wrap"
            spacing={3}
            rowGap={2}
          >
            <LabeledSelect
              label="Replica"
              value={replicaName ?? ""}
              onChange={(e) => onReplicaChange(e.target.value)}
              flexShrink={0}
              width="auto"
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
                  `${absoluteClusterPath(regionSlug, cluster)}/dataflows/${e.target.value}${replicaSearch(replicaName)}`,
                )
              }
              flex="1"
              minWidth="240px"
            >
              {dataflowId &&
                !dataflowList?.some((d) => d.id === dataflowId) && (
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
            {data && (
              <HStack flexShrink={0} height="10" spacing={2}>
                <Button size="sm" onClick={refetch} isDisabled={loading}>
                  Refresh
                </Button>
                <Text fontSize="sm" color="foreground.secondary">
                  Last fetched {data.fetchedAt.toLocaleTimeString()}
                </Text>
              </HStack>
            )}
            <IconButton
              aria-label={
                isMaximized ? "Exit full screen" : "Enter full screen"
              }
              title={isMaximized ? "Exit full screen" : "Enter full screen"}
              size="sm"
              variant="outline"
              marginLeft="auto"
              icon={
                isMaximized ? (
                  <MinimizeIcon boxSize="4" />
                ) : (
                  <MaximizeIcon boxSize="4" />
                )
              }
              onClick={() => setIsMaximized((prev) => !prev)}
            />
          </HStack>
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
              <Link
                to={`${absoluteClusterPath(regionSlug, cluster)}/dataflows${replicaSearch(replicaName)}`}
              >
                Back to dataflows
              </Link>
            </Text>
          ) : (
            <VStack flex="1" minH={0} alignItems="stretch" spacing={2}>
              <HStack
                flexShrink={0}
                flexWrap="wrap"
                alignItems="center"
                spacing={3}
                rowGap={2}
              >
                {/* At the root the Dataflow select above already names the
                  dataflow, so a trail would only repeat it. Drilled in, the
                  root crumb is the way back out. */}
                {focusedScope !== data.structure.root && (
                  <DataflowBreadcrumbs
                    structure={data.structure}
                    focusedScope={focusedScope}
                    onNavigate={setFocusedScope}
                  />
                )}
                <DataflowToolbar
                  filters={filters}
                  onFiltersChange={setFilters}
                  matchCount={allMatches.length}
                  matchIndex={activeMatchIndex}
                  onJump={onJump}
                  workerCount={data.workerCount}
                  regionExpanded={expandedScopes.size > 0}
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
                      background={`linear-gradient(to right, ${[0, 0.25, 0.5, 0.75, 1].map(heatColor).join(", ")})`}
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
                  onToggleCollapsed={() =>
                    setLirPanelCollapsed((prev) => !prev)
                  }
                  onHighlight={setLirHighlight}
                  onSelect={onLirSelect}
                />
                {/* The details panel floats over this wrapper's right edge
                  rather than sitting beside the graph, so opening it never
                  shrinks the canvas. */}
                <Box flex="1" minW={0} position="relative" display="flex">
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
                    expandedScopes={expandedScopes}
                    onToggleExpand={onToggleExpand}
                    occludedRight={
                      !selection
                        ? 0
                        : detailPanelCollapsed
                          ? COLLAPSED_STRIP_WIDTH_PX
                          : DETAIL_PANEL_WIDTH_PX
                    }
                  />
                  {selection && (
                    <NodeDetailPanel
                      selection={selection}
                      collapsed={detailPanelCollapsed}
                      onToggleCollapsed={() =>
                        setCollapsedForSelect(
                          detailPanelCollapsed ? null : selectParam,
                        )
                      }
                      onFocusRegion={setFocusedScope}
                      onJumpTo={onJumpToPeer}
                      onSelectLir={onSelectLir}
                      onSelectNode={onSelectNode}
                      workerCount={data.workerCount}
                    />
                  )}
                </Box>
              </HStack>
            </VStack>
          )}
        </VStack>
      </MaybePortal>
    </MainContentContainer>
  );
};

export default DataflowDetailPage;
