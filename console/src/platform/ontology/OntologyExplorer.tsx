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
  chakra,
  Checkbox,
  Flex,
  HStack,
  Menu,
  MenuButton,
  MenuDivider,
  MenuItem,
  MenuItemOption,
  MenuList,
  MenuOptionGroup,
  Portal,
  Switch,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { useSearchParams } from "react-router-dom";

import SearchInput from "~/components/SearchInput";
import { ChevronDownIcon, DownloadIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";
import { downloadFile } from "~/utils/downloadFile";

import { filterGraph, GraphFilters, matchesSearch } from "./filterGraph";
import { Freshness } from "./freshness";
import { layoutGraph, LayoutMode } from "./layout";
import { OntologyEdgeKind, OntologyGraph, OntologyNode } from "./model";
import { OntologyCanvas, Selection } from "./OntologyCanvas";
import { OntologySidePanel } from "./OntologySidePanel";
import { edgeKindStyle, groupColor, SIDE_PANEL_WIDTH } from "./style";

const NEIGHBORHOOD_DEPTH = 1;
/** How long a node or relationship that just appeared stays emphasized. */
const RECENTLY_ADDED_MS = 4_000;

export interface OverviewContext {
  graph: OntologyGraph;
  drawn: { nodes: number; edges: number; isolated: number };
  select: (selection: Selection) => void;
  setHighlighted: (ids: ReadonlySet<string> | null) => void;
}

export interface OntologyExplorerProps {
  /** Names the export files. */
  name: string;
  graph: OntologyGraph;
  allGroups: string[];
  groups: ReadonlySet<string> | null;
  onGroupsChange: (groups: ReadonlySet<string> | null) => void;
  defaultHiddenEdgeKinds?: OntologyEdgeKind[];
  defaultShowEdgeLabels: boolean;
  renderOverview: (context: OverviewContext) => React.ReactNode;
  getFreshness?: (node: OntologyNode) => Freshness;
  previewCluster: (node: OntologyNode) => string | null;
  /** Whether the graph is fed by live SUBSCRIBEs, shown as a badge. */
  isLive: boolean;
  suggestComments: boolean;
  /** Ids present in the first complete snapshot are not announced as new. */
  isReady: boolean;
}

/**
 * Remembers which ids the explorer has drawn, and returns the ones that
 * appeared since the first snapshot, for a few seconds each. This is how a row
 * inserted into an ontology table shows up on screen as it lands.
 */
function useRecentlyAdded(ids: string[], isReady: boolean) {
  const seen = React.useRef<Set<string> | null>(null);
  const [recent, setRecent] = React.useState<ReadonlySet<string>>(new Set());
  const idsKey = ids.join("\n");
  React.useEffect(() => {
    if (!isReady) return;
    const current = idsKey === "" ? [] : idsKey.split("\n");
    if (seen.current === null) {
      seen.current = new Set(current);
      return;
    }
    const added = current.filter((id) => !seen.current?.has(id));
    for (const id of current) seen.current.add(id);
    if (added.length === 0) return;
    setRecent((prev) => new Set([...prev, ...added]));
    const timeout = setTimeout(() => {
      setRecent((prev) => {
        const next = new Set(prev);
        for (const id of added) next.delete(id);
        return next;
      });
    }, RECENTLY_ADDED_MS);
    return () => clearTimeout(timeout);
  }, [idsKey, isReady]);
  return recent;
}

function useSelectionParams() {
  const [searchParams, setSearchParams] = useSearchParams();
  const nodeId = searchParams.get("node");
  const edgeId = searchParams.get("edge");
  const focusId = searchParams.get("focus");
  const selection: Selection = nodeId
    ? { type: "node", id: nodeId }
    : edgeId
      ? { type: "edge", id: edgeId }
      : null;
  const update = React.useCallback(
    (changes: Record<string, string | null>) =>
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev);
          for (const [key, value] of Object.entries(changes)) {
            if (value === null) next.delete(key);
            else next.set(key, value);
          }
          return next;
        },
        { replace: true },
      ),
    [setSearchParams],
  );
  const select = React.useCallback(
    (next: Selection) =>
      update({
        node: next?.type === "node" ? next.id : null,
        edge: next?.type === "edge" ? next.id : null,
      }),
    [update],
  );
  const setFocus = React.useCallback(
    (id: string | null) =>
      update(id ? { focus: id, node: id, edge: null } : { focus: null }),
    [update],
  );
  return { selection, select, focusId, setFocus };
}

export const OntologyExplorer = ({
  name,
  graph,
  allGroups,
  groups,
  onGroupsChange,
  defaultHiddenEdgeKinds = [],
  defaultShowEdgeLabels,
  renderOverview,
  getFreshness,
  previewCluster,
  isLive,
  suggestComments,
  isReady,
}: OntologyExplorerProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { selection, select, focusId, setFocus } = useSelectionParams();
  const [isPanelOpen, setIsPanelOpen] = React.useState(true);
  // Selecting something is a request to see it, so it reopens the panel.
  // Clearing the selection, as a click on empty canvas does, leaves it be.
  const selectAndShow = React.useCallback(
    (next: Selection) => {
      if (next) setIsPanelOpen(true);
      select(next);
    },
    [select],
  );
  const panelWidth = isPanelOpen ? SIDE_PANEL_WIDTH : 0;
  const [search, setSearch] = React.useState("");
  const [hiddenEdgeKinds, setHiddenEdgeKinds] = React.useState<
    ReadonlySet<OntologyEdgeKind>
  >(() => new Set(defaultHiddenEdgeKinds));
  const [showIsolated, setShowIsolated] = React.useState(true);
  const [layoutMode, setLayoutMode] =
    React.useState<LayoutMode>("hierarchical");
  const [showEdgeLabels, setShowEdgeLabels] = React.useState(
    defaultShowEdgeLabels,
  );
  const [highlighted, setHighlighted] =
    React.useState<ReadonlySet<string> | null>(null);
  const svgRef = React.useRef<SVGSVGElement>(null);

  const filters: GraphFilters = React.useMemo(
    () => ({
      groups,
      hiddenEdgeKinds,
      showIsolated,
      focus: focusId ? { nodeId: focusId, depth: NEIGHBORHOOD_DEPTH } : null,
    }),
    [groups, hiddenEdgeKinds, showIsolated, focusId],
  );
  const filtered = React.useMemo(
    () => filterGraph(graph, filters),
    [graph, filters],
  );

  // Descriptions and columns arrive after the catalog rows, so lay out on the
  // drawn ids alone: a new layout pans the canvas.
  const structureKey = React.useMemo(
    () =>
      [
        filtered.nodes.map((node) => `${node.id}@${node.group}`).join(","),
        filtered.edges.map((edge) => edge.id).join(","),
      ].join("#"),
    [filtered],
  );
  const layout = React.useMemo(
    () =>
      layoutGraph(filtered.nodes, filtered.edges, {
        mode: layoutMode,
        showEdgeLabels,
      }),
    // `filtered` is keyed by `structureKey`.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [structureKey, layoutMode, showEdgeLabels],
  );

  const recentlyAdded = useRecentlyAdded(
    React.useMemo(
      () => [
        ...graph.nodes.map((node) => node.id),
        ...graph.edges.map((edge) => edge.id),
      ],
      [graph],
    ),
    isReady,
  );

  const searchMatches = React.useMemo(() => {
    if (search.trim() === "") return null;
    return new Set(
      filtered.nodes
        .filter((node) => matchesSearch(node, search))
        .map((node) => node.id),
    );
  }, [filtered.nodes, search]);

  const nodesById = React.useMemo(
    () => new Map(graph.nodes.map((node) => [node.id, node])),
    [graph.nodes],
  );

  const edgeKinds = React.useMemo(() => {
    const counts = new Map<OntologyEdgeKind, number>();
    for (const edge of graph.edges) {
      counts.set(edge.kind, (counts.get(edge.kind) ?? 0) + 1);
    }
    return [...counts.entries()].sort((a, b) => b[1] - a[1]);
  }, [graph.edges]);

  const issueCount = filtered.issues.length;

  const exportSvg = () => {
    const svg = svgRef.current;
    if (!svg) return;
    const clone = svg.cloneNode(true) as SVGSVGElement;
    const background = document.createElementNS(
      "http://www.w3.org/2000/svg",
      "rect",
    );
    background.setAttribute("width", "100%");
    background.setAttribute("height", "100%");
    background.setAttribute("fill", colors.background.primary);
    clone.insertBefore(background, clone.firstChild);
    downloadFile(`${name}.svg`, new XMLSerializer().serializeToString(clone), {
      mimeType: "image/svg+xml",
    });
  };
  const exportJson = () => {
    downloadFile(
      `${name}.json`,
      JSON.stringify(
        {
          entities: filtered.nodes,
          relationships: filtered.edges,
          issues: filtered.issues,
        },
        null,
        2,
      ),
      { mimeType: "application/json" },
    );
  };

  const groupLabel =
    groups === null
      ? "All schemas"
      : groups.size === 1
        ? [...groups][0]
        : `${groups.size} schemas`;

  return (
    <Flex direction="column" flex={1} minHeight={0}>
      <HStack
        px={6}
        py={3}
        spacing={3}
        borderBottom="1px solid"
        borderColor={colors.border.primary}
        flexWrap="wrap"
      >
        <SearchInput
          size="sm"
          minWidth="220px"
          width="240px"
          placeholder="Search entities"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && searchMatches && searchMatches.size > 0) {
              selectAndShow({ type: "node", id: [...searchMatches][0] });
            }
          }}
          aria-label="Search entities"
        />
        <Menu closeOnSelect={false} placement="bottom-start">
          <MenuButton
            as={Button}
            size="sm"
            variant="secondary"
            rightIcon={<ChevronDownIcon />}
          >
            {groupLabel}
          </MenuButton>
          <Portal>
            <MenuList maxHeight="360px" overflowY="auto" p={2}>
              <VStack align="stretch" spacing={1} px={2}>
                <HStack justify="space-between" py={1}>
                  <Text textStyle="text-small-heavy">Schemas</Text>
                  <Button
                    size="xs"
                    variant="inline"
                    onClick={() => onGroupsChange(null)}
                  >
                    All
                  </Button>
                </HStack>
                {allGroups.map((group) => {
                  const id = `ontology-group-${group}`;
                  return (
                    <HStack key={group}>
                      <Checkbox
                        id={id}
                        isChecked={groups === null || groups.has(group)}
                        onChange={(e) => {
                          const next = new Set(groups ?? allGroups);
                          if (e.currentTarget.checked) next.add(group);
                          else next.delete(group);
                          onGroupsChange(
                            next.size === allGroups.length ? null : next,
                          );
                        }}
                      />
                      <Box
                        width="8px"
                        height="8px"
                        borderRadius="full"
                        background={groupColor(group, allGroups, colors)}
                      />
                      <chakra.label htmlFor={id} textStyle="text-ui-reg">
                        {group}
                      </chakra.label>
                    </HStack>
                  );
                })}
              </VStack>
            </MenuList>
          </Portal>
        </Menu>
        <HStack spacing={1} flexWrap="wrap">
          {edgeKinds.map(([kind, count]) => {
            const style = edgeKindStyle(kind, colors);
            const isHidden = hiddenEdgeKinds.has(kind);
            return (
              <Button
                key={kind}
                size="sm"
                variant="secondary"
                opacity={isHidden ? 0.45 : 1}
                aria-pressed={!isHidden}
                title={`${isHidden ? "Show" : "Hide"} ${style.label.toLowerCase()} relationships. ${style.description}.`}
                onClick={() =>
                  setHiddenEdgeKinds((prev) => {
                    const next = new Set(prev);
                    if (next.has(kind)) next.delete(kind);
                    else next.add(kind);
                    return next;
                  })
                }
                leftIcon={
                  <Box
                    width="14px"
                    height="0"
                    borderTop={`2px ${style.dash ? "dashed" : "solid"} ${style.color}`}
                  />
                }
              >
                {style.label}
                <Text as="span" ml={1.5} color={colors.foreground.secondary}>
                  {count}
                </Text>
              </Button>
            );
          })}
        </HStack>
        <HStack spacing={2}>
          <Switch
            id="ontology-isolated"
            size="sm"
            isChecked={showIsolated}
            onChange={(e) => setShowIsolated(e.target.checked)}
          />
          <chakra.label htmlFor="ontology-isolated" textStyle="text-ui-reg">
            Isolated
          </chakra.label>
        </HStack>
        <Box flex={1} />
        <Button
          size="sm"
          variant="secondary"
          aria-pressed={isPanelOpen}
          onClick={() => setIsPanelOpen((open) => !open)}
        >
          {isPanelOpen ? "Hide panel" : "Show panel"}
        </Button>
        <Menu placement="bottom-end">
          <MenuButton
            as={Button}
            size="sm"
            variant="secondary"
            rightIcon={<ChevronDownIcon />}
          >
            View
          </MenuButton>
          <Portal>
            <MenuList>
              <MenuOptionGroup
                title="Layout"
                type="radio"
                value={layoutMode}
                onChange={(value) => setLayoutMode(value as LayoutMode)}
              >
                <MenuItemOption value="hierarchical">
                  Hierarchical
                </MenuItemOption>
                <MenuItemOption value="circular">Circular</MenuItemOption>
              </MenuOptionGroup>
              <MenuDivider />
              <MenuOptionGroup
                title="Labels"
                type="checkbox"
                value={showEdgeLabels ? ["labels"] : []}
                onChange={(value) =>
                  setShowEdgeLabels(
                    Array.isArray(value) && value.includes("labels"),
                  )
                }
              >
                <MenuItemOption value="labels">
                  Relationship labels
                </MenuItemOption>
              </MenuOptionGroup>
            </MenuList>
          </Portal>
        </Menu>
        <Menu placement="bottom-end">
          <MenuButton
            as={Button}
            size="sm"
            variant="secondary"
            leftIcon={<DownloadIcon />}
          >
            Export
          </MenuButton>
          <Portal>
            <MenuList>
              <MenuItem onClick={exportSvg}>Image (SVG)</MenuItem>
              <MenuItem onClick={exportJson}>Ontology (JSON)</MenuItem>
            </MenuList>
          </Portal>
        </Menu>
      </HStack>

      <HStack
        px={6}
        py={2}
        spacing={5}
        borderBottom="1px solid"
        borderColor={colors.border.primary}
        background={colors.background.secondary}
      >
        <Stat label="Entities" value={filtered.nodes.length} />
        <Stat label="Relationships" value={filtered.edges.length} />
        <Stat label="Isolated" value={filtered.isolatedCount} />
        <Stat
          label="Issues"
          value={issueCount}
          color={issueCount > 0 ? colors.accent.red : undefined}
          onClick={
            issueCount > 0
              ? () => {
                  select(null);
                  setIsPanelOpen(true);
                }
              : undefined
          }
        />
        {focusId && nodesById.get(focusId) && (
          <HStack spacing={2}>
            <Text textStyle="text-small" color={colors.foreground.secondary}>
              Neighborhood of{" "}
              <Text as="span" fontWeight={600}>
                {nodesById.get(focusId)?.label}
              </Text>
            </Text>
            <Button size="xs" variant="inline" onClick={() => setFocus(null)}>
              Show all
            </Button>
          </HStack>
        )}
        <Box flex={1} />
        {isLive && <LiveBadge />}
      </HStack>

      <Box position="relative" flex={1} minHeight={0} overflow="hidden">
        {filtered.nodes.length === 0 ? (
          <Flex
            position="absolute"
            inset={0}
            right={`${panelWidth}px`}
            align="center"
            justify="center"
          >
            <Text textStyle="text-ui-reg" color={colors.foreground.secondary}>
              No entities match the current filters.
            </Text>
          </Flex>
        ) : (
          <OntologyCanvas
            nodes={filtered.nodes}
            edges={filtered.edges}
            layout={layout}
            allGroups={allGroups}
            selection={selection}
            onSelect={selectAndShow}
            highlighted={searchMatches ?? highlighted}
            recentlyAdded={recentlyAdded}
            showEdgeLabels={showEdgeLabels}
            getFreshness={getFreshness}
            rightOffset={panelWidth}
            svgRef={svgRef}
          />
        )}
        {isPanelOpen && (
          <OntologySidePanel
            selection={selection}
            onSelect={selectAndShow}
            nodesById={nodesById}
            edges={graph.edges}
            focusedNodeId={focusId}
            onFocus={setFocus}
            getFreshness={getFreshness}
            previewCluster={previewCluster}
            suggestComments={suggestComments}
            overview={renderOverview({
              graph: filtered,
              drawn: {
                nodes: filtered.nodes.length,
                edges: filtered.edges.length,
                isolated: filtered.isolatedCount,
              },
              select: selectAndShow,
              setHighlighted,
            })}
          />
        )}
      </Box>
    </Flex>
  );
};

const Stat = ({
  label,
  value,
  color,
  onClick,
}: {
  label: string;
  value: number;
  color?: string;
  onClick?: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack spacing={1.5} as={onClick ? "button" : "div"} onClick={onClick}>
      <Text textStyle="text-ui-med" color={color ?? colors.foreground.primary}>
        {value}
      </Text>
      <Text textStyle="text-small" color={colors.foreground.secondary}>
        {label}
      </Text>
    </HStack>
  );
};

const LiveBadge = () => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <HStack
      spacing={1.5}
      title="Kept current by SUBSCRIBE: changes to the catalog, the ontology tables and object freshness appear as they happen."
    >
      <Box
        width="8px"
        height="8px"
        borderRadius="full"
        background={colors.accent.green}
        sx={{
          "@keyframes ontology-live": {
            "0%": { boxShadow: `0 0 0 0 ${colors.accent.green}` },
            "70%": { boxShadow: "0 0 0 6px transparent" },
            "100%": { boxShadow: "0 0 0 0 transparent" },
          },
          animation: "ontology-live 2s infinite",
        }}
      />
      <Text textStyle="text-small-heavy" color={colors.accent.green}>
        Live
      </Text>
    </HStack>
  );
};
