// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Getter } from "jotai";
import { Emitter } from "mitt";
import React, { useContext, useMemo } from "react";
import { useLocation } from "react-router-dom";

import { parseSearchPath } from "~/api/materialize";
import { ExtendedRequestItem, SessionVariables } from "~/api/materialize/types";

import { CONNECTION_LOST_NOTICE_MESSAGE } from "./constants";
import { historyIdsAtom, HistoryItem, historyItemAtom } from "./store/shell";

export type ShellWSEmitterEvents = {
  /**
   * When the Shell's list height updates
   */
  UPDATE_HISTORY?: boolean;
};

export const shellWsEmitterEvents: Record<
  keyof ShellWSEmitterEvents,
  keyof ShellWSEmitterEvents
> = {
  UPDATE_HISTORY: "UPDATE_HISTORY",
};

/*
 * Determine whether or not the supplied history item should be displayed. This is
 * primarily used to debounce duplicate notices that could feel spammy.
 */
export async function shouldEchoHistoryItem(
  historyItem: HistoryItem,
  get: Getter,
): Promise<boolean> {
  if (
    historyItem.kind === "notice" &&
    historyItem.message === CONNECTION_LOST_NOTICE_MESSAGE
  ) {
    const historyIds = get(historyIdsAtom);
    const lastItemId = historyIds.at(-1);
    if (lastItemId !== undefined) {
      const lastItem = get(historyItemAtom(lastItemId));
      if (
        !lastItem ||
        (lastItem.kind === "notice" &&
          lastItem.message === CONNECTION_LOST_NOTICE_MESSAGE)
      ) {
        return false;
      }
    }
  }
  return true;
}

export function commandResultDisplayStateReducer(historyItem: HistoryItem) {
  return (prevHistoryItem?: HistoryItem) => {
    if (historyItem.kind === "command") {
      const displayStates = [
        ...(prevHistoryItem && prevHistoryItem.kind === "command"
          ? prevHistoryItem.commandResultsDisplayStates
          : []),
      ];
      if (historyItem.commandResults.length > displayStates.length) {
        // new results were added
        for (
          let ix = displayStates.length;
          ix < historyItem.commandResults.length;
          ix++
        ) {
          displayStates.push({
            isSubscribeManager: false,
            isFollowingSubscribeManager: true,
            currentTablePage: 0,
            currentSubscribeManagerTablePage: 0,
          });
        }
      }
      return {
        ...historyItem,
        commandResultsDisplayStates: displayStates,
      };
    }
    return historyItem;
  };
}

export type SendCallback = (params: {
  queries: ExtendedRequestItem[];
  originalCommand: string;
}) => void;

export type ShellWebsocketContextType = {
  send: SendCallback;
  commitToHistory: (historyItem: HistoryItem) => Promise<void>;
  isSocketInitializing: boolean;
  isSocketError: boolean;
  // True when socket isn't initializing and when there's no error
  isSocketAvailable: boolean;
  cacheCommand: (command: string) => void;

  // For emitting events for custom Shell events that we want to expose to clients
  on: Emitter<ShellWSEmitterEvents>["on"];
  off: Emitter<ShellWSEmitterEvents>["off"];
};

export function useShellSearchParams(
  shellRegionPath: string,
  cluster: string | undefined,
  database: string | undefined,
  search_path: string | undefined,
) {
  const location = useLocation();
  const searchParams = new URLSearchParams(location.search);
  const clusterParam = searchParams.get("cluster");
  const databaseParam = searchParams.get("database");
  const schemaParam = searchParams.get("search_path");
  const isNavigatedToShell = location.pathname === shellRegionPath;

  return useMemo(() => {
    const [schema] = parseSearchPath(search_path);
    const options: SessionVariables = {
      cluster,
      database,
      search_path: schema ? schema : undefined,
    };
    // For non-Shell routes we use the session defaults. Otherwise, override
    // the session parameters with any non-empty ones that have been provided
    // as search params.
    if (isNavigatedToShell) {
      if (!cluster && clusterParam) {
        options["cluster"] = clusterParam;
      }
      if (!database && databaseParam) {
        options["database"] = databaseParam;
      }
      if (!schema && schemaParam) {
        options["search_path"] = schemaParam;
      }
    }
    return options;
  }, [
    search_path,
    cluster,
    database,
    isNavigatedToShell,
    clusterParam,
    databaseParam,
    schemaParam,
  ]);
}

export const useShellWebsocket = () => {
  const context = useContext(ShellWebsocketContext);
  if (!context) {
    throw new Error(
      "useShellWebsocket must be used within a ShellWebsocketProvider",
    );
  }
  return context;
};

export const ShellWebsocketContext =
  React.createContext<ShellWebsocketContextType | null>(null);

export default useShellWebsocket;
