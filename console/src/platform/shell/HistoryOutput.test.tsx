// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import { Setter } from "jotai";
import mitt from "mitt";
import React from "react";

import {
  defaultRegionId,
  healthyEnvironment,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";

import { COMMAND_INTERRUPTED_MESSAGE } from "./constants";
import HistoryOutput from "./HistoryOutput";
import { createInterruptedCommandError } from "./machines/webSocketFsm";
import { CommandOutput, historyItemAtom, shellStateAtom } from "./store/shell";
import {
  ShellWebsocketContext,
  ShellWebsocketContextType,
  ShellWSEmitterEvents,
} from "./useShellWebsocket";

// The command echo reads Chakra's color mode, which the test providers do not
// install. Nothing here asserts on syntax highlighting.
vi.mock("./SyntaxHighlightedBlock", () => ({
  default: ({ value }: { value: string }) => <pre>{value}</pre>,
}));

const HISTORY_ID = "1";

const emitter = mitt<ShellWSEmitterEvents>();

const websocketContext: ShellWebsocketContextType = {
  send: vi.fn(),
  commitToHistory: vi.fn(),
  isSocketInitializing: false,
  isSocketError: false,
  isSocketAvailable: true,
  cacheCommand: vi.fn(),
  on: emitter.on,
  off: emitter.off,
};

function commandOutput(overrides: Partial<CommandOutput> = {}): CommandOutput {
  return {
    kind: "command",
    historyId: HISTORY_ID,
    command: "SHOW MATERIALIZED VIEWS;",
    statements: [{ query: "SHOW MATERIALIZED VIEWS;" }],
    commandSentTimeMs: 0,
    commandResults: [{ notices: [] }],
    commandResultsDisplayStates: [
      {
        isSubscribeManager: false,
        isFollowingSubscribeManager: true,
        currentTablePage: 0,
        currentSubscribeManagerTablePage: 0,
      },
    ],
    ...overrides,
  };
}

async function renderHistoryOutput(historyItem: CommandOutput) {
  return renderComponent(
    <ShellWebsocketContext.Provider value={websocketContext}>
      <HistoryOutput historyId={HISTORY_ID} />
    </ShellWebsocketContext.Provider>,
    {
      initializeState: async ({ set }: { set: Setter }) => {
        await setFakeEnvironment(set, defaultRegionId, healthyEnvironment);
        // Retry is only enabled once the socket is idle again.
        set(shellStateAtom, (prev) => ({
          ...prev,
          webSocketState: "readyForQuery",
        }));
        set(historyItemAtom(HISTORY_ID), historyItem);
      },
    },
  );
}

describe("HistoryOutput", () => {
  it("explains why an interrupted command needs to be retried", async () => {
    await renderHistoryOutput(
      commandOutput({
        interrupted: true,
        error: createInterruptedCommandError(),
      }),
    );

    expect(screen.getByText(COMMAND_INTERRUPTED_MESSAGE)).toBeVisible();

    const retry = screen.getByRole("button", { name: "Retry" });
    expect(retry).toBeVisible();
    expect(retry).toBeEnabled();
  });

  it("shows neither the message nor Retry for a command that was not interrupted", async () => {
    await renderHistoryOutput(commandOutput());

    expect(screen.queryByText(COMMAND_INTERRUPTED_MESSAGE)).toBeNull();
    expect(screen.queryByRole("button", { name: "Retry" })).toBeNull();
  });
});
