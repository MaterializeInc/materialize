// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import deepEqual from "fast-deep-equal";
import { atom, Getter, Setter, WritableAtom } from "jotai";
import { atomFamily, atomWithDefault, RESET } from "jotai/utils";

import { SUBSCRIBE_METADATA_COLUMNS } from "~/api/materialize/SubscribeManager";
import {
  Column,
  Error as MaterializeError,
  ExtendedRequestItem,
  Notice,
} from "~/api/materialize/types";
import storageAvailable from "~/utils/storageAvailable";

import { clearListItemHeights } from "../heightByListItem";
import { createHistoryId, HistoryId } from "../historyId";
import { WebSocketFsmState } from "../machines/webSocketFsm";
import { PlanInsights } from "../plan-insights/PlanInsightsNotice";

export type { HistoryId } from "../historyId";

export const SHELL_SIDEBAR_VISIBLE = "mz-shell-sidebar-visible";

export type SessionParameters = Partial<{
  cluster: string;
  search_path: string;
  database: string;
}>;

type ShellState = {
  tutorialVisible: boolean;
  crtEnabled: boolean;
  webSocketState: WebSocketFsmState["value"] | null;
  sessionParameters: SessionParameters;
  currentTutorialStep: number;
  connectionId: string | null;
  currentPlanInsights: {
    historyId: HistoryId;
    commandResultIndex: number;
  } | null;
};

const initialShellState = {
  tutorialVisible: getStoredSidebarVisibility(),
  crtEnabled: false,
  webSocketState: null,
  sessionParameters: {},
  currentTutorialStep: 0,
  connectionId: null,
  currentPlanInsights: null,
};

export const shellStateAtom = atomWithDefault<ShellState>(
  () => initialShellState,
);

export const historyIdsAtom = atomWithDefault<string[]>(() => []);

export type CommandResult = {
  isStreamingResult?: boolean;
  hasRows?: boolean;
  notices: Notice[];
  error?: MaterializeError;
  commandCompletePayload?: string;

  cols?: Column[];
  rows?: unknown[][];
  // Timestamp of when the server sends a `CommandComplete` or 'Error' message
  endTimeMs?: number;
};

/**
 * TODO (jun): If xstate buffer state and jotai view state differ too much, we should create
 * two different CommandOutput types for each then create a transformer when moving xstate state into
 * jotai state and vice-versa
 */
export type CommandResultsDisplayState = {
  isSubscribeManager: boolean;
  currentTablePage: number;
  currentSubscribeManagerTablePage: number;
  isFollowingSubscribeManager: boolean;
  // Insights related to why a command might be slow
  planInsights?: PlanInsights;
};

/**
 * Represents the output of line block in the shell
 */
export type CommandOutput = {
  kind: "command";
  historyId: HistoryId;

  // The initial lexed statements
  statements: ExtendedRequestItem[];
  // The query string
  command: string;
  // A timestamp of when the command was sent
  commandSentTimeMs: number;
  error?: MaterializeError;
  // When a command contains multiple statements such as "SELECT 1; SELECT 1; SELECT 1;"
  commandResults: CommandResult[];
  commandResultsDisplayStates: CommandResultsDisplayState[];
};

export type NoticeOutput = Notice & {
  kind: "notice";
  historyId: HistoryId;
};

export type LocalCommandOutput = {
  kind: "localCommand";
  historyId: HistoryId;
  command: string;
  commandResults: Array<[string, string]>;
};

export type HistoryItem = CommandOutput | NoticeOutput | LocalCommandOutput;

// TODO (robinclowers): upstream this change, these types should be exported
export type SetStateActionWithReset<Value> =
  | Value
  | typeof RESET
  | ((prev: Value) => Value | typeof RESET);

export type ResettableAtom<Value> = WritableAtom<
  Value,
  [SetStateActionWithReset<Value>],
  void
>;

export const _historyItemAtom = atom<Map<HistoryId, HistoryItem>>(new Map());

export const historyItemAtom = atomFamily<
  HistoryId,
  ResettableAtom<HistoryItem | undefined>
>((historyId) =>
  atom(
    (get) => get(_historyItemAtom).get(historyId),
    (_get, set, arg) => {
      set(_historyItemAtom, (prev) => {
        if (!arg) return prev;
        if (arg === RESET) return new Map();
        if (typeof arg === "function") {
          const oldValue = prev.get(historyId);
          const result = arg(oldValue);
          const updated = new Map(prev);
          if (typeof result === "object") {
            updated.set(historyId, result);
            return updated;
          }
          return prev;
        }
        const updated = new Map(prev);
        updated.set(historyId, arg);
        return updated;
      });
    },
  ),
);

function getStoredSidebarVisibility(): boolean {
  let visible = true;
  if (storageAvailable("localStorage")) {
    const stored = window.localStorage.getItem(SHELL_SIDEBAR_VISIBLE);
    if (stored) {
      visible = JSON.parse(stored);
    } else {
      setStoredSidebarVisibility(visible);
    }
  }
  return visible;
}

export function setStoredSidebarVisibility(visible: boolean) {
  if (storageAvailable("localStorage")) {
    window.localStorage.setItem(SHELL_SIDEBAR_VISIBLE, JSON.stringify(visible));
  }
}
/**
 *
 * A SUBSCRIBE command's output consists of an array of row where each
 * row has an `mz_diff` column which indicates the copies of the row inserted.
 * If mz_diff is negative, it indicates the copies of the row deleted.
 *
 * This function computes the current state of the output given mz_diff.
 *
 */
export function mergeMzDiffs(commandResult: CommandResult): CommandResult {
  if (
    !commandResult.isStreamingResult ||
    !commandResult.hasRows ||
    !commandResult.cols
  ) {
    return commandResult;
  }

  const reservedSubscribeColumnsIndicesByCol = commandResult.cols.reduceRight(
    (accum, col, colIndex) => {
      if (SUBSCRIBE_METADATA_COLUMNS[col.name]) {
        accum.set(col.name, colIndex);
      }
      return accum;
    },
    new Map<string, number>(),
  );

  const reservedSubscribeColumnsIndices = new Set(
    reservedSubscribeColumnsIndicesByCol.values(),
  );

  const newCols = commandResult.cols.filter(
    (_, idx) => !reservedSubscribeColumnsIndices.has(idx),
  );

  if (!commandResult.rows) {
    return {
      ...commandResult,
      cols: newCols,
      rows: [],
    };
  }

  const mzDiffIndex = reservedSubscribeColumnsIndicesByCol.get("mz_diff");
  if (mzDiffIndex === undefined) {
    return commandResult;
  }

  const rowDiffMap = commandResult.rows.reduce((accum, row) => {
    const rowWithoutReservedColumns = row.filter(
      (_, rowIndex) => !reservedSubscribeColumnsIndices.has(rowIndex),
    );

    const rowHash = JSON.stringify(rowWithoutReservedColumns);

    const diff = parseInt(row[mzDiffIndex] as string);

    let { count } = accum.get(rowHash) ?? {}; // A row's mz_diff value

    count = count ? count + diff : diff;

    if (count <= 0) {
      accum.delete(rowHash);
    } else {
      accum.set(rowHash, { count, row: rowWithoutReservedColumns });
    }

    return accum;
  }, new Map<string, { count: number; row: unknown[] }>());

  const newRows = [...rowDiffMap.entries()].reduce(
    (accum, [_, { row, count }]) => {
      for (let i = 0; i < count; i++) {
        accum.push(row);
      }

      return accum;
    },
    [] as unknown[][],
  );

  return {
    ...commandResult,
    cols: newCols,
    rows: newRows,
  };
}

export const historyItemCommandResultsSelector = atomFamily(
  (historyId: HistoryId) =>
    atom((get) => {
      const historyItem = get(historyItemAtom(historyId));

      if (!historyItem || historyItem.kind !== "command") {
        return undefined;
      }

      return historyItem.commandResults.map((commandResult, idx) => {
        const { isStreamingResult, hasRows } = commandResult;

        if (
          !isStreamingResult ||
          !hasRows ||
          historyItem.commandResultsDisplayStates[idx].isSubscribeManager
        ) {
          return commandResult;
        }
        return mergeMzDiffs(commandResult);
      });
    }),
  deepEqual,
);

export function resetShellState(get: Getter, set: Setter) {
  set(shellStateAtom, RESET);

  const historyIds = get(historyIdsAtom);
  historyIds.forEach((id) => {
    set(historyItemAtom(id), RESET);
  });

  set(historyIdsAtom, RESET);
  clearListItemHeights();
}

export function createDefaultCommandOutput(payload: {
  command: string;
  statements: ExtendedRequestItem[];
  commandSentTimeMs: number;
  commandResults?: CommandResult[];
}): CommandOutput {
  return {
    kind: "command",
    historyId: createHistoryId(),
    command: payload.command,
    statements: payload.statements,
    commandSentTimeMs: payload.commandSentTimeMs,
    commandResults: payload.commandResults ?? [],
    commandResultsDisplayStates: [],
  };
}

export function createDefaultNoticeOutput(payload: Notice): NoticeOutput {
  return {
    ...payload,
    kind: "notice" as const,
    historyId: createHistoryId(),
  };
}

export function createDefaultLocalCommandOutput(payload: {
  command: string;
  commandResults: Array<[string, string]>;
}): LocalCommandOutput {
  return {
    kind: "localCommand",
    historyId: createHistoryId(),
    command: payload.command,
    commandResults: payload.commandResults,
  };
}

export function createDefaultCommandResult(): CommandResult {
  return {
    notices: [],
  };
}

/**
 * Callback to update the display state of a command result
 */
export const updateDisplayStateAtomCallback = (
  _get: Getter,
  set: Setter,
  historyId: HistoryId,
  commandResultIdx: number,
  updater: (
    prevDisplayState: CommandResultsDisplayState,
  ) => CommandResultsDisplayState,
) => {
  set(historyItemAtom(historyId), (historyItem) => {
    if (!historyItem || historyItem.kind !== "command") {
      return historyItem;
    }

    return {
      ...historyItem,
      commandResultsDisplayStates: historyItem.commandResultsDisplayStates.map(
        (val, idx) => {
          return idx === commandResultIdx ? updater(val) : val;
        },
      ),
    };
  });
};
