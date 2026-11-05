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
  ButtonGroup,
  Flex,
  HStack,
  Select,
  SimpleGrid,
  Spinner,
  Text,
  useLatestRef,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useAtom, useAtomValue } from "jotai";
import React, { useEffect, useMemo, useRef, useState } from "react";

import { isSystemId } from "~/api/materialize";
import { BaseLayout, MainContentContainer } from "~/layouts/BaseLayout";
import { useAllClusters } from "~/store/allClusters";
import { currentRegionIdAtom } from "~/store/environments";
import { MaterializeTheme } from "~/theme";

import { CellDirection } from "./CellEditor";
import { lineCount } from "./cellLabels";
import { ChartCell } from "./ChartCell";
import { DragHandle } from "./DragHandle";
import {
  CELL_NAME_REGEX,
  expandCellReferences,
  ExpandResult,
  findCellReferences,
  renameCellReferences,
  RESERVED_CELL_NAMES,
  ResolvedReference,
} from "./expandCellReferences";
import { NewCell } from "./NewCell";
import { ActiveRun, SqlCell } from "./SqlCell";
import { useWorksheetSqlLanguage } from "./sqlCompletion";
import { hasCode } from "./sqlTokens";
import {
  addToHistory,
  explorerOpenAtom,
  explorerQueryAtom,
  newCell,
  newChartSpec,
  worksheetAtomFamily,
  WorksheetCell,
  worksheetHistoryAtomFamily,
  worksheetStorageId,
  worksheetTabsAtomFamily,
} from "./store";
import { runsLive, toCellQuery } from "./subscribeStatement";
import { WorksheetExplorer } from "./WorksheetExplorer";
import { WorksheetRuntime } from "./worksheetRuntime";
import { WorksheetTabs } from "./WorksheetTabs";

/** Unwraps a SUBSCRIBE statement, then expands `{{name}}` references. */
function expandCellQuery(
  source: string,
  resolve: (name: string) => ResolvedReference,
): ExpandResult {
  const cellQuery = toCellQuery(source);
  if (cellQuery.error !== undefined) {
    return { error: cellQuery.error, dependencies: [] };
  }
  return expandCellReferences(cellQuery.query, resolve);
}

/** The data type of a cell being dragged to reorder it. */
const CELL_DRAG_TYPE = "application/x-materialize-worksheet-cell";

/** Moves a Run all past `id`, ending it after the last cell. */
function advanceRunAll(
  queue: { ids: string[]; index: number },
  id: string,
): { ids: string[]; index: number } | undefined {
  if (queue.ids[queue.index] !== id) return queue;
  return queue.index + 1 < queue.ids.length
    ? { ...queue, index: queue.index + 1 }
    : undefined;
}

/** Why `name` cannot be referenced, or undefined if it can. */
function validateCellName(name: string, sameNameCount: number) {
  if (!CELL_NAME_REGEX.test(name)) {
    return "Use lowercase letters, digits and underscores, not starting with a digit";
  }
  if (RESERVED_CELL_NAMES.has(name)) return `"${name}" is reserved`;
  if (sameNameCount > 1) return "Another cell has this name";
  return undefined;
}

/**
 * The worksheet route. The data explorer, when open, fills the sidebar the
 * way it does in the Data section.
 */
export const WorksheetRoute = () => {
  const isExplorerOpen = useAtomValue(explorerOpenAtom);
  return (
    <BaseLayout sectionNav={isExplorerOpen ? <WorksheetExplorer /> : undefined}>
      <WorksheetPage />
    </BaseLayout>
  );
};

export const WorksheetPage = () => {
  const regionId = useAtomValue(currentRegionIdAtom);
  if (!regionId) return null;
  return <WorksheetTabPage regionId={regionId} />;
};

const WorksheetTabPage = ({ regionId }: { regionId: string }) => {
  const { activeId } = useAtomValue(worksheetTabsAtomFamily(regionId));
  const storageId = worksheetStorageId(regionId, activeId);
  // Remount per worksheet so runs and committed sources never cross
  // worksheets or regions. Switching tabs stops the hidden tab's live cells.
  return (
    <Worksheet
      key={storageId}
      regionId={regionId}
      tabId={activeId}
      storageId={storageId}
    />
  );
};

export interface WorksheetProps {
  regionId: string;
  tabId: string;
  storageId: string;
}

export const Worksheet = ({ regionId, tabId, storageId }: WorksheetProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [worksheet, setWorksheet] = useAtom(worksheetAtomFamily(storageId));
  // The source each cell last ran with. References expand against these, so
  // editing a cell does not disturb its dependents until it is run.
  const [committed, setCommitted] = useState<Record<string, string>>({});
  const [active, setActive] = useState<Record<string, ActiveRun>>({});
  const [history, setHistory] = useAtom(worksheetHistoryAtomFamily(regionId));
  const [runtime] = useState(() => new WorksheetRuntime());
  useEffect(() => () => runtime.dispose(), [runtime]);

  const { cells } = worksheet;
  const sqlCells = cells.filter((c) => c.kind !== "chart");
  // Line numbers run on across cells in the order Run all executes them, so
  // a line number names one line of the whole worksheet.
  const lineNumbering = useMemo(() => {
    const firstLines = new Map<string, number>();
    let next = 1;
    for (const cell of cells) {
      if (cell.kind === "chart") continue;
      firstLines.set(cell.id, next);
      next += lineCount(cell.source);
    }
    return { firstLines, next, digits: Math.max(2, String(next).length) };
  }, [cells]);
  const nameCounts = new Map<string, number>();
  for (const c of sqlCells) {
    nameCounts.set(c.name, (nameCounts.get(c.name) ?? 0) + 1);
  }
  const cellNamed = (name: string) =>
    nameCounts.get(name) === 1
      ? sqlCells.find((c) => c.name === name)
      : undefined;

  const resolveCommitted = (name: string): ResolvedReference => {
    const count = nameCounts.get(name) ?? 0;
    if (count === 0) return { error: `No cell is named {{${name}}}` };
    if (count > 1) return { error: `More than one cell is named {{${name}}}` };
    const source = committed[(cellNamed(name) as WorksheetCell).id];
    if (source === undefined) return { error: `Run {{${name}}} first` };
    // A reference to a SUBSCRIBE cell splices in its query.
    return { source: toCellQuery(source).query ?? source };
  };

  const updateCell = (id: string, update: Partial<WorksheetCell>) =>
    setWorksheet((w) => ({
      ...w,
      cells: w.cells.map((c) => (c.id === id ? { ...c, ...update } : c)),
    }));

  const runCell = (cell: WorksheetCell, { recordHistory = true } = {}) => {
    // Commit upstream cells that have never run, so running a dependent
    // works without running each of its inputs first.
    const commits: Record<string, string> = { [cell.id]: cell.source };
    const visit = (source: string) => {
      for (const name of findCellReferences(source)) {
        const upstream = cellNamed(name);
        if (
          upstream &&
          !(upstream.id in commits) &&
          !(upstream.id in committed)
        ) {
          commits[upstream.id] = upstream.source;
          visit(upstream.source);
        }
      }
    };
    visit(cell.source);
    if (recordHistory) setHistory((prev) => addToHistory(prev, cell.source));
    setCommitted((prev) => ({ ...prev, ...commits }));
    setActive((prev) => ({
      ...prev,
      [cell.id]: {
        mode: runsLive(cell.source) ? "live" : "once",
        token: (prev[cell.id]?.token ?? 0) + 1,
      },
    }));
  };

  // Run all goes top to bottom, one cell at a time, so a cell that creates
  // an object finishes before a later cell reads it. A cell counts as done
  // when it finishes, fails, or, if live, delivers its first snapshot.
  const [runAll, setRunAll] = useState<{ ids: string[]; index: number }>();
  const runAllCellId = runAll?.ids[runAll.index];
  const runCellRef = useLatestRef(runCell);
  const cellsRef = useLatestRef(cells);
  useEffect(() => {
    if (!runAllCellId) return;
    const cell = cellsRef.current.find((c) => c.id === runAllCellId);
    if (cell) {
      runCellRef.current(cell, { recordHistory: false });
    } else {
      // Deleted while queued.
      setRunAll((q) => q && advanceRunAll(q, runAllCellId));
    }
  }, [runAllCellId, runAll?.index, cellsRef, runCellRef]);
  const onCellSettled = (id: string) =>
    setRunAll((q) => q && advanceRunAll(q, id));
  const startRunAll = () => {
    const ids = sqlCells.filter(inRunAll).map((c) => c.id);
    if (ids.length > 0) setRunAll({ ids, index: 0 });
  };

  // The cell the results pane shows. Running a cell selects it, the way a
  // SQL worksheet shows the statement that last ran. Pinned cells stay in
  // view beside it instead, so selecting one leaves the shown cell alone.
  const [selectedId, setSelectedId] = useState<string>();
  const select = (cell: WorksheetCell) => {
    if (!cell.pinned) setSelectedId(cell.id);
  };
  const [resultsPane, setResultsPane] = useState<HTMLDivElement | null>(null);
  const [paneBody, setPaneBody] = useState<HTMLDivElement | null>(null);
  const resultTabs = sqlCells.filter((c) => c.id in committed && !c.pinned);
  const pinnedCells = sqlCells.filter((c) => c.id in committed && c.pinned);
  const shownId = resultTabs.some((c) => c.id === selectedId)
    ? selectedId
    : resultTabs.at(-1)?.id;
  // NOTE: a ref callback only ever adds, because one that removed on null
  // would set state on every render and loop. Stale entries are never read,
  // since only pinned cells look theirs up.
  const [pinSlots, setPinSlots] = useState<Record<string, HTMLDivElement>>({});
  const pinSlotRef = (id: string) => (el: HTMLDivElement | null) => {
    if (el)
      setPinSlots((prev) => (prev[id] === el ? prev : { ...prev, [id]: el }));
  };
  const paneTargetOf = (cell: WorksheetCell) =>
    cell.pinned
      ? (pinSlots[cell.id] ?? null)
      : cell.id === shownId
        ? resultsPane
        : null;
  const scrollToCell = (id: string | undefined) =>
    scrollRef.current
      ?.querySelector(`[data-cell="${id}"]`)
      ?.scrollIntoView({ block: "nearest", behavior: "smooth" });
  // Tab walks the editors top to bottom, ending in the new cell. Focusing a
  // cell shows its results.
  const moveFocus = (fromId: string, direction: CellDirection) => {
    const order = [...sqlCells.map((c) => c.id), "new"];
    const target =
      order[order.indexOf(fromId) + (direction === "next" ? 1 : -1)];
    if (!target) return;
    scrollRef.current
      ?.querySelector<HTMLElement>(`[data-cell="${target}"] .cm-content`)
      ?.focus({ preventScroll: true });
    // Focus alone only scrolls the caret into view, which can leave the cell
    // under the sticky header or cut off at the results pane.
    scrollToCell(target);
  };
  const showResultsOf = (id: string) => {
    setSelectedId(id);
    scrollToCell(id);
  };
  const tabStripRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    tabStripRef.current
      ?.querySelector(`[data-tab="${shownId}"]`)
      ?.scrollIntoView({ block: "nearest", inline: "nearest" });
  }, [shownId]);
  // NOTE: a DOM listener, because the pane's content is portaled in from the
  // cell, and React delivers its events up the cell's tree, not the pane's.
  const scrollToCellRef = useLatestRef(scrollToCell);
  useEffect(() => {
    if (!paneBody) return;
    const onPointerDown = (event: PointerEvent) => {
      const slot = (event.target as Element).closest<HTMLElement>(
        "[data-slot]",
      );
      if (slot?.dataset.slot) scrollToCellRef.current(slot.dataset.slot);
    };
    paneBody.addEventListener("pointerdown", onPointerDown);
    return () => paneBody.removeEventListener("pointerdown", onPointerDown);
  }, [paneBody, scrollToCellRef]);

  const stopCell = (id: string) =>
    setActive(({ [id]: _stopped, ...rest }) => rest);

  const nextName = (prefix: string) => {
    const names = new Set(cells.map((c) => c.name));
    let n = 1;
    while (names.has(`${prefix}_${n}`)) n++;
    return `${prefix}_${n}`;
  };

  const addCell = (source: string, asChart: boolean) => {
    const cell: WorksheetCell = {
      ...newCell(nextName("cell"), source),
      ...(asChart ? { view: "chart" as const, chart: newChartSpec() } : {}),
    };
    setWorksheet((w) => ({ ...w, cells: [...w.cells, cell] }));
    runCell(cell);
    setSelectedId(cell.id);
  };

  const removeCell = (id: string) => {
    stopCell(id);
    setWorksheet((w) => ({ ...w, cells: w.cells.filter((c) => c.id !== id) }));
  };

  /** Moves a cell to just before or after another. */
  const moveCellTo = (id: string, targetId: string, after: boolean) =>
    setWorksheet((w) => {
      if (id === targetId) return w;
      const next = [...w.cells];
      const from = next.findIndex((c) => c.id === id);
      if (from < 0) return w;
      const [cell] = next.splice(from, 1);
      const target = next.findIndex((c) => c.id === targetId);
      if (target < 0) return w;
      next.splice(after ? target + 1 : target, 0, cell);
      return { ...w, cells: next };
    });

  const [dragging, setDragging] = useState<string>();
  const [dropTarget, setDropTarget] = useState<{
    id: string;
    after: boolean;
  }>();
  const endDrag = () => {
    setDragging(undefined);
    setDropTarget(undefined);
  };

  /** Renames a SQL cell and every reference to it. */
  const renameCell = (id: string, name: string): string | undefined => {
    const cell = cells.find((c) => c.id === id);
    if (!cell) return undefined;
    const error = validateCellName(
      name,
      sqlCells.filter((c) => c.id !== id && c.name === name).length + 1,
    );
    if (error) return error;
    const oldName = cell.name;
    const rename = (source: string) =>
      renameCellReferences(source, oldName, name);
    setWorksheet((w) => ({
      ...w,
      cells: w.cells.map((c) => {
        if (c.id === id) return { ...c, name };
        if (c.kind === "chart") {
          return c.chart?.source === oldName
            ? { ...c, chart: { ...c.chart, source: name } }
            : c;
        }
        return { ...c, source: rename(c.source) };
      }),
    }));
    setCommitted((prev) =>
      Object.fromEntries(
        Object.entries(prev).map(([cellId, source]) => [
          cellId,
          rename(source),
        ]),
      ),
    );
    return undefined;
  };

  const language = useWorksheetSqlLanguage(
    Array.from(new Set(sqlCells.map((c) => c.name))),
  );

  const { data: clusters } = useAllClusters();
  const clusterNames = useMemo(
    () =>
      clusters
        .filter((c) => !isSystemId(c.id))
        .map((c) => c.name)
        .sort((a, b) => a.localeCompare(b)),
    [clusters],
  );

  const scrollRef = useRef<HTMLDivElement>(null);
  const previousCellCount = useRef(cells.length);
  useEffect(() => {
    // Bring a cell added from the bottom bar into view.
    if (cells.length > previousCellCount.current) {
      scrollRef.current?.scrollTo({
        top: scrollRef.current.scrollHeight,
        behavior: "smooth",
      });
    }
    previousCellCount.current = cells.length;
  }, [cells.length]);

  const columns = worksheet.columns === 2 ? 2 : 1;

  const [isExplorerOpen, setExplorerOpen] = useAtom(explorerOpenAtom);
  const [explorerQuery, setExplorerQuery] = useAtom(explorerQueryAtom);
  const addCellRef = useLatestRef(addCell);
  useEffect(() => {
    if (explorerQuery === undefined) return;
    setExplorerQuery(undefined);
    addCellRef.current(explorerQuery, false);
  }, [explorerQuery, setExplorerQuery, addCellRef]);

  // Dashboard: the cells not hidden from it, as titled results. Full screen
  // is a separate step from inside it.
  const rootRef = useRef<HTMLDivElement>(null);
  const [presenting, setPresenting] = useState(false);
  const [isFullscreen, setFullscreen] = useState(false);
  useEffect(() => {
    const onFullscreenChange = () =>
      setFullscreen(document.fullscreenElement === rootRef.current);
    document.addEventListener("fullscreenchange", onFullscreenChange);
    return () =>
      document.removeEventListener("fullscreenchange", onFullscreenChange);
  }, []);
  const toggleFullscreen = () => {
    if (document.fullscreenElement) {
      void document.exitFullscreen();
    } else {
      rootRef.current?.requestFullscreen?.().catch(() => {
        // Refused, for example inside an iframe. The dashboard still works.
      });
    }
  };
  const startPresenting = () => {
    setPresenting(true);
    // A dashboard of cells that never ran would be empty.
    if (Object.keys(active).length === 0) startRunAll();
  };
  const stopPresenting = () => {
    if (document.fullscreenElement) void document.exitFullscreen();
    setPresenting(false);
  };
  const [paneHeight, setPaneHeight] = useState(320);
  const startPaneResize = (event: React.PointerEvent) => {
    const startY = event.clientY;
    const startHeight = paneHeight;
    const onMove = (move: PointerEvent) =>
      setPaneHeight(
        Math.min(
          Math.max(startHeight + startY - move.clientY, 120),
          window.innerHeight * 0.8,
        ),
      );
    const onUp = () => {
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
    };
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
  };
  const dashboardCells = cells.filter(
    (c) => c.kind === "chart" || !c.resultsHidden,
  );
  const { tabs } = useAtomValue(worksheetTabsAtomFamily(regionId));
  const tabTitle = tabs.find((t) => t.id === tabId)?.title ?? "Worksheet";
  const liveCount = Object.values(active).filter(
    (a) => a.mode === "live",
  ).length;

  const layoutToggle = (
    <ButtonGroup size="xs" isAttached variant="secondary">
      {([1, 2] as const).map((n) => (
        <Button
          key={n}
          aria-pressed={columns === n}
          fontWeight={columns === n ? "600" : "400"}
          bg={columns === n ? colors.background.tertiary : undefined}
          title={n === 1 ? "One column" : "Two columns"}
          onClick={() => setWorksheet((w) => ({ ...w, columns: n }))}
        >
          {n === 1 ? "1 column" : "2 columns"}
        </Button>
      ))}
    </ButtonGroup>
  );

  const presentingHeader = (
    <HStack
      position="sticky"
      top="0"
      zIndex="3"
      bg={colors.background.primary}
      justifyContent="space-between"
      py="2"
      borderBottomWidth="1px"
      borderColor={colors.border.primary}
    >
      <HStack spacing="3">
        <Text textStyle="heading-xs">{tabTitle}</Text>
        {liveCount > 0 && (
          <Text textStyle="text-small" color={colors.accent.green}>
            ● {liveCount} live
          </Text>
        )}
      </HStack>
      <HStack spacing="2">
        {layoutToggle}
        <Button
          size="xs"
          variant="secondary"
          isDisabled={Boolean(runAll)}
          title="Run every cell again"
          onClick={startRunAll}
        >
          {runAll
            ? `Refreshing ${runAll.index + 1} of ${runAll.ids.length}`
            : "Refresh"}
        </Button>
        <Button
          size="xs"
          variant="secondary"
          aria-pressed={isFullscreen}
          onClick={toggleFullscreen}
        >
          {isFullscreen ? "Exit full screen" : "Full screen"}
        </Button>
        <Button size="xs" variant="primary" onClick={stopPresenting}>
          Back to worksheet
        </Button>
      </HStack>
    </HStack>
  );

  return (
    <MainContentContainer mt="0" px="0" pb="0" overflow="hidden">
      <Flex ref={rootRef} flex="1" minH="0" bg={colors.background.primary}>
        <Flex flex="1" minW="0" flexDirection="column">
          <Box ref={scrollRef} flex="1" minH="0" overflowY="auto" px="4">
            {presenting ? (
              presentingHeader
            ) : (
              <HStack
                position="sticky"
                top="0"
                // Above the results tables' sticky headers.
                zIndex="3"
                bg={colors.background.primary}
                justifyContent="space-between"
                // Narrow windows put the controls on a second row rather
                // than pushing them off the edge.
                flexWrap="wrap"
                rowGap="2"
                py="2"
                borderBottomWidth="1px"
                borderColor={colors.border.primary}
              >
                <HStack spacing="2" minW="0">
                  <Button
                    size="xs"
                    variant={isExplorerOpen ? "secondary" : "inline"}
                    aria-pressed={isExplorerOpen}
                    title={
                      isExplorerOpen
                        ? "Hide the data explorer"
                        : "Show the data explorer"
                    }
                    onClick={() => setExplorerOpen((open) => !open)}
                    flexShrink={0}
                  >
                    Explorer
                  </Button>
                  <WorksheetTabs
                    regionId={regionId}
                    activeHasCells={cells.length > 0}
                  />
                </HStack>
                <HStack
                  spacing="3"
                  minW="0"
                  flexWrap="wrap"
                  rowGap="2"
                  justifyContent="flex-end"
                  ml="auto"
                >
                  {runAll ? (
                    <Button
                      size="xs"
                      variant="secondary"
                      leftIcon={<Spinner size="xs" />}
                      title="Stop after the cell that is running"
                      onClick={() => setRunAll(undefined)}
                    >
                      Running {runAll.index + 1} of {runAll.ids.length} · Stop
                    </Button>
                  ) : (
                    <Button
                      size="xs"
                      variant="primary"
                      isDisabled={!sqlCells.some(inRunAll)}
                      title="Run every SQL cell, top to bottom"
                      onClick={startRunAll}
                    >
                      Run all
                    </Button>
                  )}
                  <HStack spacing="1.5">
                    <Text
                      as="label"
                      htmlFor="worksheet-cluster"
                      textStyle="text-small"
                      color={colors.foreground.secondary}
                    >
                      Cluster
                    </Text>
                    <Select
                      id="worksheet-cluster"
                      size="xs"
                      width="auto"
                      minW="36"
                      value={worksheet.cluster}
                      title="The cluster every cell runs on"
                      onChange={(e) =>
                        setWorksheet((w) => ({ ...w, cluster: e.target.value }))
                      }
                    >
                      <option value="">Session default</option>
                      {worksheet.cluster &&
                        !clusterNames.includes(worksheet.cluster) && (
                          <option value={worksheet.cluster}>
                            {worksheet.cluster} (not found)
                          </option>
                        )}
                      {clusterNames.map((name) => (
                        <option key={name} value={name}>
                          {name}
                        </option>
                      ))}
                    </Select>
                  </HStack>
                  <Button
                    size="xs"
                    variant="secondary"
                    isDisabled={cells.length === 0}
                    title="Show the cells' results as a dashboard"
                    onClick={startPresenting}
                  >
                    Dashboard
                  </Button>
                </HStack>
              </HStack>
            )}
            {cells.length === 0 ? (
              <EmptyWorksheet />
            ) : presenting && dashboardCells.length === 0 ? (
              <Text
                py="16"
                textAlign="center"
                color={colors.foreground.secondary}
              >
                Every cell is hidden from the dashboard.
              </Text>
            ) : (
              <SimpleGrid
                columns={presenting ? columns : 1}
                spacingX="4"
                // Cells are told apart by their left bars, so they need
                // space between them. On a dashboard, borders do that.
                spacingY={presenting ? "0" : "4"}
                alignItems="start"
                pt={presenting ? "0" : "3"}
              >
                {(presenting ? dashboardCells : cells).map((cell, index) => {
                  const dragHandle = (
                    <DragHandle
                      onDragStart={(event) => {
                        setDragging(cell.id);
                        event.dataTransfer.effectAllowed = "move";
                        // NOTE: a private type, because the editors accept
                        // dropped plain text and would insert it.
                        event.dataTransfer.setData(CELL_DRAG_TYPE, cell.id);
                        const cellElement =
                          event.currentTarget.closest("[data-cell]");
                        if (cellElement) {
                          event.dataTransfer.setDragImage(cellElement, 24, 16);
                        }
                      }}
                      onDragEnd={endDrag}
                      onMove={(delta) => {
                        const neighbor = cells[index + delta];
                        if (neighbor)
                          moveCellTo(cell.id, neighbor.id, delta > 0);
                      }}
                    />
                  );
                  const actions = {
                    dragHandle,
                    onRemove: () => removeCell(cell.id),
                  };
                  const isDropTarget = dropTarget?.id === cell.id;
                  const content =
                    cell.kind === "chart" && cell.chart ? (
                      <ChartCell
                        cell={{ ...cell, chart: cell.chart }}
                        runtime={runtime}
                        sqlCellNames={sqlCells.map((c) => c.name)}
                        onChange={(update) => updateCell(cell.id, update)}
                        presenting={presenting}
                        {...actions}
                      />
                    ) : (
                      <SqlCell
                        cell={cell}
                        runtime={runtime}
                        nameError={validateCellName(
                          cell.name,
                          nameCounts.get(cell.name) ?? 0,
                        )}
                        expanded={
                          committed[cell.id] === undefined
                            ? undefined
                            : expandCellQuery(
                                committed[cell.id],
                                resolveCommitted,
                              )
                        }
                        active={active[cell.id]}
                        cluster={worksheet.cluster}
                        onChange={(update) => updateCell(cell.id, update)}
                        onRename={(name) => renameCell(cell.id, name)}
                        onRun={() => {
                          runCell(cell);
                          select(cell);
                        }}
                        onRefresh={() =>
                          runCell(cell, { recordHistory: false })
                        }
                        onStop={() => stopCell(cell.id)}
                        onSettled={() => onCellSettled(cell.id)}
                        presenting={presenting}
                        language={language}
                        lineNumbers={{
                          first: lineNumbering.firstLines.get(cell.id) ?? 1,
                          digits: lineNumbering.digits,
                        }}
                        resultsPane={paneTargetOf(cell)}
                        onTab={(direction) => moveFocus(cell.id, direction)}
                        selected={paneTargetOf(cell) !== null}
                        onTogglePin={() => {
                          updateCell(cell.id, { pinned: !cell.pinned });
                          // Unpinned, it takes over the pane it left.
                          if (cell.pinned) setSelectedId(cell.id);
                        }}
                        {...actions}
                      />
                    );
                  return (
                    <Box
                      key={cell.id}
                      data-cell={cell.id}
                      // Keeps a cell scrolled to from the results pane clear
                      // of the sticky header.
                      scrollMarginTop="14"
                      scrollMarginBottom="4"
                      position="relative"
                      onFocus={() => {
                        if (cell.id in committed) select(cell);
                      }}
                      opacity={dragging === cell.id ? 0.4 : 1}
                      onDragOver={(event) => {
                        if (!dragging || dragging === cell.id) return;
                        event.preventDefault();
                        const box = event.currentTarget.getBoundingClientRect();
                        const after = event.clientY > box.top + box.height / 2;
                        if (
                          dropTarget?.id !== cell.id ||
                          dropTarget.after !== after
                        ) {
                          setDropTarget({ id: cell.id, after });
                        }
                      }}
                      onDrop={(event) => {
                        event.preventDefault();
                        if (dragging && dropTarget) {
                          moveCellTo(dragging, dropTarget.id, dropTarget.after);
                        }
                        endDrag();
                      }}
                    >
                      {isDropTarget && (
                        // Where the dragged cell will land, in the gap
                        // between cells.
                        <Box
                          position="absolute"
                          left="0"
                          right="0"
                          height="2px"
                          borderRadius="full"
                          bg={colors.accent.purple}
                          {...(dropTarget.after
                            ? { bottom: "-9px" }
                            : { top: "-9px" })}
                        />
                      )}
                      {content}
                    </Box>
                  );
                })}
              </SimpleGrid>
            )}
            {!presenting && (
              <NewCell
                onSubmit={addCell}
                history={history}
                language={language}
                onTab={(direction) => moveFocus("new", direction)}
                lineNumbers={{
                  first: lineNumbering.next,
                  digits: lineNumbering.digits,
                }}
              />
            )}
          </Box>
          {!presenting && (
            <Flex
              direction="column"
              height={`${paneHeight}px`}
              flexShrink={0}
              borderTopWidth="1px"
              borderColor={colors.border.primary}
              position="relative"
            >
              <Box
                position="absolute"
                top="-3px"
                left="0"
                right="0"
                height="6px"
                cursor="row-resize"
                zIndex="4"
                title="Drag to resize"
                onPointerDown={startPaneResize}
              />
              <HStack
                ref={tabStripRef}
                role="tablist"
                aria-label="Results"
                spacing="0"
                px="4"
                flexShrink={0}
                overflowX="auto"
                onKeyDown={(event) => {
                  if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") {
                    return;
                  }
                  const index = resultTabs.findIndex((c) => c.id === shownId);
                  const next =
                    resultTabs[index + (event.key === "ArrowLeft" ? -1 : 1)];
                  if (!next) return;
                  event.preventDefault();
                  showResultsOf(next.id);
                  event.currentTarget
                    .querySelector<HTMLElement>(`[data-tab="${next.id}"]`)
                    ?.focus();
                }}
                borderBottomWidth="1px"
                borderColor={colors.border.primary}
              >
                {resultTabs.length === 0 && (
                  <Text
                    py="1.5"
                    textStyle="text-small"
                    color={colors.foreground.tertiary}
                  >
                    Results of the cell you run show here.
                  </Text>
                )}
                {resultTabs.map((c) => {
                  const isShown = c.id === shownId;
                  const isLive = active[c.id]?.mode === "live";
                  return (
                    <Button
                      key={c.id}
                      size="xs"
                      variant="inline"
                      borderRadius="0"
                      height="8"
                      px="3"
                      fontFamily="mono"
                      fontWeight={isShown ? "600" : "400"}
                      color={
                        isShown
                          ? colors.foreground.primary
                          : colors.foreground.secondary
                      }
                      borderBottomWidth="2px"
                      borderColor={
                        isShown ? colors.accent.purple : "transparent"
                      }
                      role="tab"
                      data-tab={c.id}
                      // Many tabs scroll the strip rather than squeeze.
                      flexShrink={0}
                      aria-selected={isShown}
                      tabIndex={isShown ? 0 : -1}
                      onClick={() => showResultsOf(c.id)}
                    >
                      {isLive && (
                        <Text as="span" color={colors.accent.green} mr="1">
                          ●
                        </Text>
                      )}
                      {c.name}
                    </Button>
                  );
                })}
              </HStack>
              <Flex ref={setPaneBody} flex="1" minH="0">
                <Box
                  ref={setResultsPane}
                  role="tabpanel"
                  data-slot={shownId}
                  flex="1"
                  minW="0"
                  px="4"
                  pb="2"
                />
                {pinnedCells.map((c) => (
                  <Flex
                    key={c.id}
                    direction="column"
                    flex="1"
                    minW="0"
                    borderLeftWidth="1px"
                    borderColor={colors.border.primary}
                    px="4"
                    pb="2"
                    data-slot={c.id}
                    aria-label={`${c.name}, pinned`}
                    role="region"
                  >
                    <Text
                      pt="1.5"
                      textStyle="text-small"
                      fontFamily="mono"
                      fontWeight="600"
                      noOfLines={1}
                    >
                      {active[c.id]?.mode === "live" && (
                        <Text as="span" color={colors.accent.green} mr="1">
                          ●
                        </Text>
                      )}
                      {c.name}
                    </Text>
                    <Box ref={pinSlotRef(c.id)} flex="1" minH="0" />
                  </Flex>
                ))}
              </Flex>
            </Flex>
          )}
        </Flex>
      </Flex>
    </MainContentContainer>
  );
};

const inRunAll = (cell: WorksheetCell) =>
  !cell.skipInRunAll && hasCode(cell.source);

const EmptyWorksheet = () => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <VStack
      py="16"
      spacing="2"
      color={colors.foreground.secondary}
      textAlign="center"
    >
      <Text textStyle="heading-xs" color={colors.foreground.primary}>
        This worksheet is empty
      </Text>
      <Text textStyle="text-base">
        Write SQL in the editor below and press Shift-Enter to add it as a cell.
      </Text>
      <Text textStyle="text-small" maxW="lg">
        Run a cell with ▶ or Shift-Enter. Cells can use each other as{" "}
        {"{{name}}"}. In the results pane, ⚡ keeps a cell&apos;s results
        updating and the chart button plots them. A cell&apos;s first comment
        line becomes its title on the dashboard.
      </Text>
    </VStack>
  );
};
