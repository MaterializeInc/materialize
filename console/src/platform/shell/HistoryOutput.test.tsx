// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { act, screen, waitFor, within } from "@testing-library/react";
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
import { EXECUTION_TIME_NOTICE_CODE } from "./executionTime";
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

  it.each([
    [
      '{"duration_us":2000,"kind":"completed"}',
      "Returned in 148.0ms · 2.0ms in Materialize",
    ],
    ["not json", "Returned in 148.0ms"],
  ])(
    "never prints an execution time notice as a notice line (detail %s)",
    async (detail, footer) => {
      await renderHistoryOutput(
        commandOutput({
          commandSentTimeMs: 1000,
          commandResults: [
            {
              notices: [
                {
                  code: EXECUTION_TIME_NOTICE_CODE,
                  message: "execution time: 2.000 ms",
                  severity: "Notice",
                  detail,
                },
              ],
              hasRows: false,
              commandCompletePayload: "CREATE TABLE",
              endTimeMs: 1148,
            },
          ],
        }),
      );

      expect(screen.getByText(footer)).toBeVisible();
      expect(screen.queryByText(/execution time:/)).toBeNull();
    },
  );

  describe("timing tooltip", () => {
    const executionTimeNotice = (kind: string) => ({
      code: EXECUTION_TIME_NOTICE_CODE,
      message: "execution time: 12.000 ms",
      severity: "Notice" as const,
      detail: JSON.stringify({ duration_us: 12000, kind }),
    });
    const displayState = {
      isSubscribeManager: false,
      isFollowingSubscribeManager: true,
      currentTablePage: 0,
      currentSubscribeManagerTablePage: 0,
    };

    async function openTooltip(button: HTMLElement) {
      act(() => button.focus());
      const tooltip = await screen.findByRole("tooltip");
      // The tooltip fades in.
      await waitFor(() => expect(tooltip).toBeVisible());
      return tooltip;
    }

    it("opens on keyboard focus and explains each timing in its own row", async () => {
      await renderHistoryOutput(
        commandOutput({
          commandSentTimeMs: 1000,
          commandResults: [
            {
              notices: [executionTimeNotice("first_row")],
              hasRows: false,
              commandCompletePayload: "SELECT 2",
              endTimeMs: 1148,
            },
          ],
        }),
      );

      const tooltip = await openTooltip(
        screen.getByRole("button", { name: "Timing details" }),
      );

      expect(within(tooltip).getByText("Returned in")).toBeVisible();
      expect(
        within(tooltip).getByText(/measured from sending the input\.$/),
      ).toBeVisible();
      expect(within(tooltip).getByText("In Materialize")).toBeVisible();
      expect(
        within(tooltip).getByText(/does not include sending the rows/),
      ).toBeVisible();
    });

    it("says a later statement's round trip can be shorter than its time in Materialize", async () => {
      await renderHistoryOutput(
        commandOutput({
          statements: [{ query: "SELECT 1" }, { query: "SELECT 2" }],
          commandSentTimeMs: 1000,
          commandResults: [
            {
              notices: [executionTimeNotice("first_row")],
              hasRows: false,
              commandCompletePayload: "SELECT 1",
              endTimeMs: 1100,
            },
            {
              notices: [executionTimeNotice("first_row")],
              hasRows: false,
              commandCompletePayload: "SELECT 1",
              endTimeMs: 1104,
            },
          ],
          commandResultsDisplayStates: [displayState, displayState],
        }),
      );

      const [, second] = screen.getAllByRole("button", {
        name: "Timing details",
      });
      expect(
        within(await openTooltip(second)).getByText(
          /measured from the previous result\. It can be shorter than the time in Materialize/,
        ),
      ).toBeVisible();
    });

    it("has only the round trip row without an execution time", async () => {
      await renderHistoryOutput(
        commandOutput({
          commandSentTimeMs: 1000,
          commandResults: [
            {
              notices: [],
              hasRows: false,
              commandCompletePayload: "CREATE TABLE",
              endTimeMs: 1148,
            },
          ],
        }),
      );

      const tooltip = await openTooltip(
        screen.getByRole("button", { name: "Timing details" }),
      );

      expect(within(tooltip).getByText("Returned in")).toBeVisible();
      expect(within(tooltip).queryByText("In Materialize")).toBeNull();
    });
  });
});
