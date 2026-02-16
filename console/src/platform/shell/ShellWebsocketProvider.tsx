// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { captureException } from "@sentry/react";
import { interpret, InterpreterStatus, StateMachine } from "@xstate/fsm";
import { useAtom } from "jotai";
import { useAtomCallback } from "jotai/utils";
import debounce from "lodash.debounce";
import mitt from "mitt";
import React, { useCallback, useMemo, useRef, useState } from "react";

import { SHELL_APPLICATION_NAME } from "~/api/materialize";
import {
  MaterializeWebsocket,
  ParameterStatus,
} from "~/api/materialize/MaterializeWebsocket";
import { useAutomaticallyConnectSocket } from "~/hooks/useAutomaticallyConnectSocket";
import { useFlags } from "~/hooks/useFlags";
import { useSyncObjectToSearchParams } from "~/hooks/useSyncObjectToSearchParams";
import { shellPath } from "~/platform/routeHelpers";
import { regionIdToSlug } from "~/store/cloudRegions";
import { useCurrentEnvironmentHttpAddress } from "~/store/environments";

import { appendToCache } from "./commandCache";
import {
  COMMAND_RESULT_MAX_SIZE_BYTES,
  CONNECTION_LOST_NOTICE_MESSAGE,
  JOTAI_DEBOUNCE_WAIT_MS,
} from "./constants";
import WebSocketFsm, {
  getLatestCommandOutputClone,
  WebSocketFsmContext,
  WebSocketFsmEvent,
  WebSocketFsmState,
} from "./machines/webSocketFsm";
import {
  createDefaultNoticeOutput,
  historyIdsAtom,
  HistoryItem,
  historyItemAtom,
  shellStateAtom,
} from "./store/shell";
import { useResetShellStateOnRegionChange } from "./store/useResetShellStateOnRegionChange";
import useResetPromptCache from "./useResetPromptCache";
import {
  commandResultDisplayStateReducer,
  SendCallback,
  ShellWebsocketContext,
  ShellWSEmitterEvents,
  shellWsEmitterEvents,
  shouldEchoHistoryItem,
  useShellSearchParams,
} from "./useShellWebsocket";

export const ShellWebsocketProvider = ({
  children,
  organizationId,
  regionId,
}: {
  children: React.ReactNode;
  organizationId?: string;
  regionId: string;
}) => {
  const [shellState, setShellState] = useAtom(shellStateAtom);
  const flags = useFlags();
  const shellRegionPath = shellPath(regionIdToSlug(regionId));

  useResetPromptCache({ organizationId, regionId });
  const [emitter] = useState(() => mitt<ShellWSEmitterEvents>());

  const stateMachineRef = useRef<StateMachine.Service<
    WebSocketFsmContext,
    WebSocketFsmEvent,
    WebSocketFsmState
  > | null>(null);

  const {
    sessionParameters: { cluster, database, search_path },
  } = shellState;

  const additionalAuthOptions = useShellSearchParams(
    shellRegionPath,
    cluster,
    database,
    search_path,
  );

  // This object is only used in refs, so there's no need to memoize it
  const sessionVariables = {
    application_name: SHELL_APPLICATION_NAME,
    max_query_result_size: COMMAND_RESULT_MAX_SIZE_BYTES,
    emit_plan_insights_notice: flags["plan-insights-3846"] ? "on" : "off",
    ...additionalAuthOptions,
  };

  useSyncObjectToSearchParams(additionalAuthOptions, shellRegionPath);

  const httpAddress = useCurrentEnvironmentHttpAddress();

  const commitToHistory = useAtomCallback<Promise<void>, [HistoryItem]>(
    React.useCallback(
      async (get, set, historyItem) => {
        if (!(await shouldEchoHistoryItem(historyItem, get))) return;
        set(
          historyItemAtom(historyItem.historyId),
          // We cannot send an 'updater' callback as an argument since the atom hasn't been set yet
          commandResultDisplayStateReducer(historyItem)(),
        );
        set(historyIdsAtom, (currentHistoryIds) => [
          ...currentHistoryIds,
          historyItem.historyId,
        ]);

        emitter.emit(shellWsEmitterEvents.UPDATE_HISTORY);
      },
      [emitter],
    ),
  );

  const updateHistoryItem = useAtomCallback(
    React.useCallback(
      async (_get, set, historyItem: HistoryItem) => {
        const id = historyItem.historyId;
        set(historyItemAtom(id), commandResultDisplayStateReducer(historyItem));

        emitter.emit(shellWsEmitterEvents.UPDATE_HISTORY);
      },
      [emitter],
    ),
  );

  const getStateMachine = useCallback(() => {
    if (stateMachineRef.current !== null) {
      return stateMachineRef.current as StateMachine.Service<
        WebSocketFsmContext,
        WebSocketFsmEvent,
        WebSocketFsmState
      >;
    }

    const stateMachine = interpret(WebSocketFsm);

    stateMachineRef.current = stateMachine;
    return stateMachine;
  }, []);

  const saveLatestCommandOutputToGlobalState = useCallback(() => {
    const stateMachine = getStateMachine();

    const historyItem = getLatestCommandOutputClone(stateMachine);
    updateHistoryItem(historyItem);
  }, [
    getStateMachine, // Should not change
    updateHistoryItem, // Should not change
  ]);

  // It's important to debounce this function since otherwise, we could be cloning thousands of rows per message
  // which would negatively impact performance drastically.
  const saveLatestCommandOutputWhenStreamingToGlobalState = useMemo(
    () =>
      debounce(() => {
        saveLatestCommandOutputToGlobalState();
      }, JOTAI_DEBOUNCE_WAIT_MS),
    [saveLatestCommandOutputToGlobalState],
  );

  const socketRef = React.useRef(
    new MaterializeWebsocket({
      httpAddress,
      sessionVariables: sessionVariables,
      onOpen: () => {
        const stateMachine = getStateMachine();

        let prevWebSocketState: WebSocketFsmState["value"] | null = null;

        stateMachine.subscribe(({ value: newWebsocketState }) => {
          if (
            prevWebSocketState === null ||
            prevWebSocketState !== newWebsocketState
          ) {
            setShellState((prevState) => ({
              ...prevState,
              webSocketState: newWebsocketState,
            }));
          }
          prevWebSocketState = newWebsocketState;
        });

        stateMachine.subscribe((state) => {
          if (
            !state.changed &&
            !state.matches("initialState") &&
            stateMachine.status !== InterpreterStatus.Stopped
          ) {
            captureException(
              new Error("Unsuccessful state machine transition"),
              {
                extra: {
                  xState: state,
                },
              },
            );
          }
        });

        stateMachine.start();
      },
      onMessage: (result) => {
        const stateMachine = getStateMachine();

        const saveSessionParameterToGlobalState = (
          parameter: ParameterStatus,
        ) => {
          setShellState((state) => {
            return {
              ...state,
              sessionParameters: {
                ...state.sessionParameters,
                [parameter.name]: parameter.value,
              },
            };
          });
        };
        try {
          switch (result.type) {
            case "BackendKeyData":
              if (result.payload.conn_id) {
                setShellState((prevState) => ({
                  ...prevState,
                  connectionId: `${result.payload.conn_id}`,
                }));
              }
              break;
            case "ReadyForQuery":
              stateMachine.send("READY_FOR_QUERY");
              break;
            case "CommandStarting":
              if (result.payload.is_streaming) {
                stateMachine.send({
                  type: "COMMAND_STARTING_IS_STREAMING",
                  hasRows: result.payload.has_rows,
                });
              } else if (result.payload.has_rows) {
                stateMachine.send("COMMAND_STARTING_HAS_ROWS");
              } else {
                stateMachine.send("COMMAND_STARTING_DEFAULT");
              }
              break;
            case "Rows":
              stateMachine.send({ type: "ROWS", rows: result.payload });

              break;
            case "Row":
              if (stateMachine.state.matches("commandInProgressStreaming")) {
                stateMachine.send({ type: "ROW", row: result.payload });
                saveLatestCommandOutputWhenStreamingToGlobalState();
              } else if (
                stateMachine.state.matches("commandInProgressHasRows")
              ) {
                stateMachine.send({ type: "ROW", row: result.payload });
              }
              break;
            case "CommandComplete":
              stateMachine.send({
                type: "COMMAND_COMPLETE",
                commandCompletePayload: result.payload,
              });

              saveLatestCommandOutputToGlobalState();
              break;
            case "Notice":
              if (stateMachine.state.matches("readyForQuery")) {
                commitToHistory(createDefaultNoticeOutput(result.payload));
              } else {
                stateMachine.send({
                  type: "NOTICE",
                  notice: result.payload,
                });

                saveLatestCommandOutputToGlobalState();
              }
              break;
            case "ParameterStatus":
              saveSessionParameterToGlobalState(result.payload);
              break;
            case "Error":
              stateMachine.send({
                type: "ERROR",
                error: result.payload,
              });
              saveLatestCommandOutputToGlobalState();
              break;
          }
        } catch (error) {
          captureException(error);
        }
      },
      onClose: () => {
        const stateMachine = getStateMachine();
        stateMachine.stop();
        commitToHistory(
          createDefaultNoticeOutput({
            message: CONNECTION_LOST_NOTICE_MESSAGE,
            severity: "Info",
          }),
        );
      },
    }),
  );

  const { error: socketError, readyForQuery } = React.useSyncExternalStore(
    socketRef.current.onChange,
    socketRef.current.getSnapshot,
  );

  const isSocketInitializing = !socketRef.current.isInitialized;
  const isSocketError = Boolean(socketError);
  const isSocketAvailable = !isSocketInitializing && !isSocketError;

  const cacheCommand = useCallback(
    (command: string) => {
      appendToCache({ organizationId, regionId, command });
    },
    [organizationId, regionId],
  );

  const send: SendCallback = useCallback(
    async ({ queries, originalCommand }) => {
      if (readyForQuery) {
        const stateMachine = getStateMachine();
        stateMachine.send({
          type: "SEND",
          command: originalCommand,
          statements: queries,
        });
        socketRef.current.send({ queries });
        cacheCommand(originalCommand);
        commitToHistory(getLatestCommandOutputClone(stateMachine));
      }
    },
    [readyForQuery, getStateMachine, cacheCommand, commitToHistory],
  );

  useAutomaticallyConnectSocket({
    target: socketRef.current,
    sessionVariables: sessionVariables,
  });

  useResetShellStateOnRegionChange();

  return (
    <ShellWebsocketContext.Provider
      value={{
        on: emitter.on,
        off: emitter.off,
        send,
        commitToHistory,
        isSocketAvailable,
        isSocketError,
        isSocketInitializing,
        cacheCommand,
      }}
    >
      {children}
    </ShellWebsocketContext.Provider>
  );
};
