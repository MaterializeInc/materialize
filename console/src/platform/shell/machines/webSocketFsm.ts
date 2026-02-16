// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { assign, createMachine, StateMachine } from "@xstate/fsm";

import {
  Column,
  Error,
  ExtendedRequestItem,
  Notice,
} from "~/api/materialize/types";
import { assert } from "~/util";

import {
  CommandOutput,
  CommandResult,
  createDefaultCommandOutput,
  createDefaultCommandResult,
} from "../store/shell";

type SendEvent = {
  type: "SEND";
  command: string;
  statements: ExtendedRequestItem[];
};
type CommandStartingIsStreamingEvent = {
  type: "COMMAND_STARTING_IS_STREAMING";
  hasRows: boolean;
};
type CommandStartingHasRowsEvent = { type: "COMMAND_STARTING_HAS_ROWS" };
type CommandStartingDefaultEvent = { type: "COMMAND_STARTING_DEFAULT" };
type CommandCompleteEvent = {
  type: "COMMAND_COMPLETE";
  commandCompletePayload: string;
};
type RowsEvent = { type: "ROWS"; rows: { columns: Column[] } };
type RowEvent = { type: "ROW"; row: unknown[] };
type ErrorEvent = { type: "ERROR"; error: Error };
type NoticeEvent = { type: "NOTICE"; notice: Notice };

type ReadyForQueryEvent = { type: "READY_FOR_QUERY" };

export type WebSocketFsmContext = {
  latestCommandOutput?: CommandOutput;
};

export type WebSocketFsmEvent =
  | SendEvent
  | CommandStartingIsStreamingEvent
  | CommandStartingHasRowsEvent
  | CommandStartingDefaultEvent
  | CommandCompleteEvent
  | RowsEvent
  | RowEvent
  | ErrorEvent
  | NoticeEvent
  | ReadyForQueryEvent;

export type WebSocketFsmState =
  | {
      value: "initialState";
      context: WebSocketFsmContext;
    }
  | {
      value: "readyForQuery";
      context: WebSocketFsmContext;
    }
  | {
      value: "commandSent";
      context: WebSocketFsmContext;
    }
  | {
      value: "commandInProgressDefault";
      context: WebSocketFsmContext;
    }
  | {
      value: "commandInProgressHasRows";
      context: WebSocketFsmContext;
    }
  | {
      value: "commandInProgressStreaming";
      context: WebSocketFsmContext;
    };

function completeCommandResult(commandResult: CommandResult) {
  commandResult.endTimeMs = performance.now();
}

function addError({
  latestCommandOutput,
  error,
}: {
  latestCommandOutput: CommandOutput;
  error: Error;
}) {
  latestCommandOutput.error = error;
}

function getLatestCommandResult(latestCommandOutput?: CommandOutput) {
  assert(latestCommandOutput);
  const latestCommandResult = latestCommandOutput.commandResults.at(-1);
  assert(latestCommandResult);
  return latestCommandResult;
}

const addNoticeToLatestCommandResult = assign<WebSocketFsmContext, NoticeEvent>(
  {
    latestCommandOutput: ({ latestCommandOutput }, event) => {
      const latestCommandResult = getLatestCommandResult(latestCommandOutput);

      latestCommandResult.notices.push(event.notice);
      return latestCommandOutput;
    },
  },
);

const addErrorDuringCommandInProgress = assign<WebSocketFsmContext, ErrorEvent>(
  {
    latestCommandOutput: ({ latestCommandOutput }, event) => {
      assert(latestCommandOutput);
      addError({ latestCommandOutput, error: event.error });
      const latestCommandResult = getLatestCommandResult(latestCommandOutput);
      completeCommandResult(latestCommandResult);
      return latestCommandOutput;
    },
  },
);

const addRowsToLatestCommandResult = assign<WebSocketFsmContext, RowsEvent>({
  latestCommandOutput: ({ latestCommandOutput }, event) => {
    const latestCommandResult = getLatestCommandResult(latestCommandOutput);
    latestCommandResult.cols = event.rows.columns;
    return latestCommandOutput;
  },
});

const addRowToLatestCommandResult = assign<WebSocketFsmContext, RowEvent>({
  latestCommandOutput: ({ latestCommandOutput }, event) => {
    const latestCommandResult = getLatestCommandResult(latestCommandOutput);
    if (latestCommandResult.rows) {
      latestCommandResult.rows.push(event.row);
    }
    return latestCommandOutput;
  },
});

const completeLatestCommandResult = assign<
  WebSocketFsmContext,
  CommandCompleteEvent
>({
  latestCommandOutput: ({ latestCommandOutput }, event) => {
    assert(latestCommandOutput);
    const latestCommandResult = getLatestCommandResult(latestCommandOutput);
    latestCommandResult.commandCompletePayload = event.commandCompletePayload;
    completeCommandResult(latestCommandResult);

    // Add a new command result to the latestCommandOutput if we expect any more because this is the only ack we get

    if (
      latestCommandOutput.commandResults.length !==
      latestCommandOutput.statements.length
    ) {
      // We have received all the command results for this command
      latestCommandOutput.commandResults.push(createDefaultCommandResult());
    }
    return latestCommandOutput;
  },
});

export const webSocketFsm = createMachine<
  WebSocketFsmContext,
  WebSocketFsmEvent,
  WebSocketFsmState
>({
  id: "webSocketFsm",
  initial: "initialState",
  context: {},
  states: {
    initialState: {
      on: {
        READY_FOR_QUERY: {
          target: "readyForQuery",
          actions: assign({}),
        },
      },
    },
    readyForQuery: {
      on: {
        SEND: {
          target: "commandSent",
          actions: assign({
            latestCommandOutput: (_, event) =>
              createDefaultCommandOutput({
                command: event.command,
                commandSentTimeMs: performance.now(),
                statements: event.statements,
                // We assume that the first command starts when we receive "commandSent"
                commandResults: [createDefaultCommandResult()],
              }),
          }),
        },
      },
    },
    commandSent: {
      on: {
        COMMAND_STARTING_DEFAULT: {
          target: "commandInProgressDefault",
          actions: assign({
            latestCommandOutput: ({ latestCommandOutput }) => {
              const latestCommandResult =
                getLatestCommandResult(latestCommandOutput);
              latestCommandResult.hasRows = false;
              latestCommandResult.isStreamingResult = false;

              return latestCommandOutput;
            },
          }),
        },
        COMMAND_STARTING_HAS_ROWS: {
          target: "commandInProgressHasRows",
          actions: assign({
            latestCommandOutput: ({ latestCommandOutput }) => {
              const latestCommandResult =
                getLatestCommandResult(latestCommandOutput);
              latestCommandResult.hasRows = true;
              latestCommandResult.isStreamingResult = false;
              latestCommandResult.rows = [];

              return latestCommandOutput;
            },
          }),
        },
        COMMAND_STARTING_IS_STREAMING: {
          target: "commandInProgressStreaming",
          actions: assign({
            latestCommandOutput: ({ latestCommandOutput }, event) => {
              const latestCommandResult =
                getLatestCommandResult(latestCommandOutput);
              latestCommandResult.hasRows = event.hasRows;
              if (event.hasRows) {
                latestCommandResult.rows = [];
              }
              latestCommandResult.isStreamingResult = true;

              return latestCommandOutput;
            },
          }),
        },
        NOTICE: {
          target: "commandSent",
          actions: addNoticeToLatestCommandResult,
        },
        ERROR: {
          target: "commandSent",
          actions: assign({
            latestCommandOutput: ({ latestCommandOutput }, event) => {
              assert(latestCommandOutput);
              addError({ latestCommandOutput, error: event.error });
              return latestCommandOutput;
            },
          }),
        },

        READY_FOR_QUERY: {
          target: "readyForQuery",
          actions: assign({}),
        },
      },
    },
    commandInProgressDefault: {
      on: {
        COMMAND_COMPLETE: {
          target: "commandSent",
          actions: completeLatestCommandResult,
        },
        NOTICE: {
          target: "commandInProgressHasRows",
          actions: addNoticeToLatestCommandResult,
        },
        ERROR: {
          target: "commandSent",
          actions: addErrorDuringCommandInProgress,
        },
      },
    },
    commandInProgressHasRows: {
      on: {
        COMMAND_COMPLETE: {
          target: "commandSent",
          actions: completeLatestCommandResult,
        },
        NOTICE: {
          target: "commandInProgressHasRows",
          actions: addNoticeToLatestCommandResult,
        },
        ERROR: {
          target: "commandSent",
          actions: addErrorDuringCommandInProgress,
        },
        ROWS: {
          target: "commandInProgressHasRows",
          actions: addRowsToLatestCommandResult,
        },
        ROW: {
          target: "commandInProgressHasRows",
          actions: addRowToLatestCommandResult,
        },
      },
    },
    commandInProgressStreaming: {
      on: {
        NOTICE: {
          target: "commandInProgressStreaming",
          actions: addNoticeToLatestCommandResult,
        },
        ERROR: {
          target: "commandSent",
          actions: addErrorDuringCommandInProgress,
        },
        ROWS: {
          target: "commandInProgressStreaming",
          actions: addRowsToLatestCommandResult,
        },
        ROW: {
          target: "commandInProgressStreaming",
          actions: addRowToLatestCommandResult,
        },
        COMMAND_COMPLETE: {
          target: "commandSent",
          actions: completeLatestCommandResult,
        },
      },
    },
  },
});

export function isCommandProcessing(state: WebSocketFsmState["value"]) {
  return state !== "initialState" && state !== "readyForQuery";
}

/**
 *
 * A function to create a deep copy of a state machine's latest command output.
 * Since our actions mutate the state machine's context, we need to clone it
 * when passing it to Jotai's immutable data structures
 *
 */
export function getLatestCommandOutputClone(
  stateMachine: StateMachine.Service<
    WebSocketFsmContext,
    WebSocketFsmEvent,
    WebSocketFsmState
  >,
): CommandOutput {
  assert(stateMachine.state.context.latestCommandOutput);
  return structuredClone(stateMachine.state.context.latestCommandOutput);
}

export default webSocketFsm;
