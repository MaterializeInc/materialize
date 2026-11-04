// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { atom } from "jotai";
import { atomFamily, atomWithStorage, createJSONStorage } from "jotai/utils";
import { v4 as uuidv4 } from "uuid";

import { isQueryStatement, wrapInSubscribe } from "./subscribeStatement";

export type ChartMark = "bar" | "line" | "area" | "dot" | "pie";

export interface ChartSpec {
  /** The name of the SQL cell whose rows are plotted. */
  source: string;
  mark: ChartMark;
  x: string;
  y: string;
  /** Empty means a single series. */
  color: string;
}

export interface WorksheetCell {
  id: string;
  /** Absent on cells saved before chart cells existed. */
  kind?: "sql" | "chart";
  /**
   * For a chart cell, what it plots. For a SQL cell, its chart settings,
   * kept while it shows a table so switching back restores them.
   */
  chart?: ChartSpec;
  /** How a SQL cell shows its results. Absent means a table. */
  view?: "table" | "chart";
  /** Referenced from other cells as `{{name}}`. */
  name: string;
  source: string;
  /**
   * Unused: a cell is live when its SQL is a SUBSCRIBE. Saves from before
   * that are converted on load.
   */
  live: boolean;
  /** Shows only the first line of the query, or only a chart's controls. */
  collapsed?: boolean;
  /** Hides a SQL cell's results while editing. Present always shows them. */
  resultsHidden?: boolean;
  /** Keeps the cell's results in the pane beside whichever cell is shown. */
  pinned?: boolean;
  /** Run all passes over the cell. It still runs on its own. */
  skipInRunAll?: boolean;
}

export interface Worksheet {
  cells: WorksheetCell[];
  /** Empty means the session's default cluster. */
  cluster: string;
  columns?: 1 | 2;
}

export const newCell = (name: string, source = ""): WorksheetCell => ({
  id: uuidv4(),
  name,
  source,
  live: false,
});

/** Chart settings with the axes left for the chart to pick from its rows. */
export const newChartSpec = (source = ""): ChartSpec => ({
  source,
  mark: "bar",
  x: "",
  y: "",
  color: "",
});

export const newChartCell = (name: string, source: string): WorksheetCell => ({
  ...newCell(name),
  kind: "chart",
  chart: { source, mark: "bar", x: "", y: "", color: "" },
});

const CHART_MARKS: ChartMark[] = ["bar", "line", "area", "dot", "pie"];

const createDefaultWorksheet = (): Worksheet => ({
  cluster: "",
  cells: [
    newCell(
      "cluster_replicas",
      "-- Replicas of every cluster\n" +
        "SELECT c.name AS cluster, r.name AS replica, r.size\n" +
        "FROM mz_clusters c JOIN mz_cluster_replicas r ON r.cluster_id = c.id",
    ),
    newCell(
      "replicas_per_cluster",
      wrapInSubscribe(
        "-- Replicas per cluster, kept live\n" +
          "SELECT cluster, count(*) AS replicas\n" +
          "FROM {{cluster_replicas}}\nGROUP BY cluster",
      ),
    ),
  ],
});

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const asString = (value: unknown, fallback = "") =>
  typeof value === "string" ? value : fallback;

function normalizeChart(value: unknown, fallbackSource: string): ChartSpec {
  const chart = isRecord(value) ? value : {};
  const mark = asString(chart.mark) as ChartMark;
  return {
    source: asString(chart.source, fallbackSource),
    mark: CHART_MARKS.includes(mark) ? mark : "bar",
    x: asString(chart.x),
    y: asString(chart.y),
    color: asString(chart.color),
  };
}

/**
 * Coerces whatever is in storage into a valid worksheet. Storage outlives
 * code versions and can be edited by hand, so nothing about it is trusted.
 */
// The starter cells' first comments used to be editing tips, which read
// badly as dashboard titles. Worksheets that still have them get the titles
// the starter cells use now.
const RETIRED_STARTER_COMMENTS: Record<string, string> = {
  "-- Run a cell with ▶ or Shift-Enter. Other cells can use it by name.":
    "-- Replicas of every cluster",
  "-- SUBSCRIBE keeps results updating as the data changes. ⚡ adds or removes it.":
    "-- Replicas per cluster, kept live",
};

function retireStarterComment(source: string) {
  const newline = source.indexOf("\n");
  const firstLine = newline < 0 ? source : source.slice(0, newline);
  const replacement = RETIRED_STARTER_COMMENTS[firstLine];
  return replacement === undefined
    ? source
    : replacement + source.slice(firstLine.length);
}

export function normalizeWorksheet(value: unknown): Worksheet {
  if (!isRecord(value) || !Array.isArray(value.cells)) {
    return createDefaultWorksheet();
  }
  const cells = value.cells.filter(isRecord).map((cell): WorksheetCell => {
    const source = retireStarterComment(asString(cell.source));
    const base: WorksheetCell = {
      id: asString(cell.id) || uuidv4(),
      name: asString(cell.name),
      // Cells used to go live through a flag rather than their SQL.
      source:
        cell.live === true && isQueryStatement(source)
          ? wrapInSubscribe(source)
          : source,
      live: false,
      collapsed: cell.collapsed === true,
      ...(cell.resultsHidden === true ? { resultsHidden: true } : {}),
      ...(cell.pinned === true ? { pinned: true } : {}),
      ...(cell.skipInRunAll === true ? { skipInRunAll: true } : {}),
    };
    if (cell.kind === "chart") {
      return { ...base, kind: "chart", chart: normalizeChart(cell.chart, "") };
    }
    return {
      ...base,
      kind: "sql",
      ...(cell.view === "chart" ? { view: "chart" as const } : {}),
      ...(isRecord(cell.chart)
        ? { chart: normalizeChart(cell.chart, "") }
        : {}),
    };
  });
  return {
    cells,
    cluster: asString(value.cluster),
    columns: value.columns === 2 ? 2 : 1,
  };
}

const worksheetStorage = createJSONStorage<Worksheet>(() => localStorage);

/**
 * A query the data explorer asks the open worksheet to add and run. The
 * explorer sits in the page's sidebar, outside the worksheet.
 */
export const explorerQueryAtom = atom<string | undefined>(undefined);

/** The data explorer's width in pixels, as last resized. */
export const explorerWidthAtom = atomWithStorage(
  "worksheet:explorerWidth",
  320,
  undefined,
  { getOnInit: true },
);

/** Whether the data explorer panel is open beside worksheets. */
export const explorerOpenAtom = atomWithStorage(
  "worksheet:explorerOpen",
  false,
  undefined,
  { getOnInit: true },
);

/** The first tab, which keeps the storage key from before tabs existed. */
export const DEFAULT_TAB_ID = "default";

export interface WorksheetTab {
  id: string;
  title: string;
}

export interface WorksheetTabs {
  activeId: string;
  tabs: WorksheetTab[];
}

/** Identifies a worksheet's storage, unique across regions. */
export function worksheetStorageId(regionId: string, tabId: string) {
  return tabId === DEFAULT_TAB_ID ? regionId : `${regionId}/${tabId}`;
}

export const worksheetStorageKey = (storageId: string) =>
  `worksheet:${storageId}`;

export const EMPTY_WORKSHEET: Worksheet = {
  cells: [],
  cluster: "",
  columns: 1,
};

function normalizeTabs(value: unknown): WorksheetTabs {
  const fallback: WorksheetTabs = {
    activeId: DEFAULT_TAB_ID,
    tabs: [{ id: DEFAULT_TAB_ID, title: "Worksheet" }],
  };
  if (!isRecord(value) || !Array.isArray(value.tabs)) return fallback;
  const tabs = value.tabs
    .filter(isRecord)
    .map((t) => ({ id: asString(t.id), title: asString(t.title, "Worksheet") }))
    .filter((t) => t.id);
  if (tabs.length === 0) return fallback;
  const activeId = asString(value.activeId);
  return {
    tabs,
    activeId: tabs.some((t) => t.id === activeId) ? activeId : tabs[0].id,
  };
}

/** A region's open worksheets and which one is showing. */
export const worksheetTabsAtomFamily = atomFamily((regionId: string) =>
  atomWithStorage<WorksheetTabs>(
    `worksheet:tabs:${regionId}`,
    normalizeTabs(undefined),
    {
      ...createJSONStorage<WorksheetTabs>(() => localStorage),
      getItem: (key, initialValue) => {
        try {
          const stored = localStorage.getItem(key);
          return stored === null
            ? initialValue
            : normalizeTabs(JSON.parse(stored));
        } catch {
          return initialValue;
        }
      },
    },
    { getOnInit: true },
  ),
);

/**
 * One worksheet, kept in this browser's local storage. Edits in another tab
 * arrive through the storage event and update open editors.
 */
export const worksheetAtomFamily = atomFamily((storageId: string) =>
  atomWithStorage<Worksheet>(
    worksheetStorageKey(storageId),
    createDefaultWorksheet(),
    {
      ...worksheetStorage,
      getItem: (key, initialValue) => {
        let stored: unknown;
        try {
          stored = JSON.parse(localStorage.getItem(key) ?? "null");
        } catch {
          return initialValue;
        }
        return stored === null ? initialValue : normalizeWorksheet(stored);
      },
      subscribe: (key, callback, initialValue) =>
        worksheetStorage.subscribe?.(
          key,
          (value) => callback(normalizeWorksheet(value)),
          initialValue,
        ) ?? (() => {}),
    },
    { getOnInit: true },
  ),
);

const MAX_HISTORY_ENTRIES = 200;

/** Queries run in a region's worksheet, newest first and without repeats. */
export const worksheetHistoryAtomFamily = atomFamily((regionId: string) =>
  atomWithStorage<string[]>(
    `worksheet:history:${regionId}`,
    [],
    {
      ...createJSONStorage<string[]>(() => localStorage),
      getItem: (key, initialValue) => {
        try {
          const stored: unknown = JSON.parse(localStorage.getItem(key) ?? "[]");
          return Array.isArray(stored)
            ? stored.filter(
                (entry): entry is string => typeof entry === "string",
              )
            : initialValue;
        } catch {
          return initialValue;
        }
      },
    },
    { getOnInit: true },
  ),
);

/** Moves `source` to the front of `history`, dropping its older copy. */
export function addToHistory(history: readonly string[], source: string) {
  const entry = source.trim();
  if (!entry) return [...history];
  return [entry, ...history.filter((h) => h !== entry)].slice(
    0,
    MAX_HISTORY_ENTRIES,
  );
}
