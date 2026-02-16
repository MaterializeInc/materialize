// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { interpret } from "@xstate/fsm";

import { ErrorCode, MzDataType } from "~/api/materialize/types";

import { createHistoryId } from "../historyId";
import webSocketFsm from "./webSocketFsm";

const MOCKED_HISTORY_ID = "1";

const MOCKED_PERFORMANCE_NOW = 0;

vi.mock("~/platform/shell/historyId");
vi.mocked(createHistoryId).mockImplementation(() => MOCKED_HISTORY_ID);

describe("webSocketFsm", () => {
  beforeAll(() => {
    vi.stubGlobal("performance", {
      now: vi.fn().mockReturnValue(MOCKED_PERFORMANCE_NOW),
    });
  });

  afterAll(() => {
    vi.unstubAllGlobals();
  });

  describe("integration tests", () => {
    it("SELECT 5;", () => {
      const machine = interpret(webSocketFsm);

      machine.start();
      machine.send("READY_FOR_QUERY");
      expect(machine.state.matches("readyForQuery")).toBeTruthy();

      machine.send({
        type: "SEND",
        command: "SELECT 5;",
        statements: [{ query: "SELECT 5;" }],
      });
      expect(machine.state.matches("commandSent")).toBeTruthy();

      expect(
        machine.state.context.latestCommandOutput?.commandResults.length,
      ).toBe(1);

      machine.send("COMMAND_STARTING_HAS_ROWS");
      expect(machine.state.matches("commandInProgressHasRows")).toBeTruthy();

      const mockColumn = {
        name: "?column?",
        type_oid: MzDataType.int4,
        type_len: 4,
        type_mod: -1,
      };

      machine.send({
        type: "ROWS",
        rows: {
          columns: [mockColumn],
        },
      });
      expect(machine.state.matches("commandInProgressHasRows")).toBeTruthy();

      machine.send({
        type: "ROW",
        row: [5],
      });

      expect(machine.state.matches("commandInProgressHasRows")).toBeTruthy();

      machine.send({
        type: "COMMAND_COMPLETE",
        commandCompletePayload: "SELECT 5",
      });

      expect(machine.state.matches("commandSent")).toBeTruthy();

      machine.send("READY_FOR_QUERY");
      expect(machine.state.matches("readyForQuery")).toBeTruthy();

      expect(machine.state.context).toEqual({
        latestCommandOutput: {
          kind: "command",
          historyId: MOCKED_HISTORY_ID,
          command: "SELECT 5;",
          commandSentTimeMs: MOCKED_PERFORMANCE_NOW,
          commandResults: [
            {
              cols: [mockColumn],
              rows: [[5]],
              endTimeMs: MOCKED_PERFORMANCE_NOW,
              isStreamingResult: false,
              hasRows: true,
              notices: [],
              commandCompletePayload: "SELECT 5",
            },
          ],
          commandResultsDisplayStates: [],
          statements: [
            {
              query: "SELECT 5;",
            },
          ],
        },
      });
    });
  });

  describe("unit tests", () => {
    it("initial state should be an empty object", () => {
      const machine = interpret(webSocketFsm);
      machine.start();
      expect(machine.state.matches("initialState")).toBeTruthy();
      expect(machine.state.context).toEqual({});
    });

    it("error in the commandInProgressDefault state", () => {
      const machine = interpret(webSocketFsm);
      machine.start();
      machine.send("READY_FOR_QUERY");
      machine.send({
        type: "SEND",
        command: "SELECT nuke_materialize();",
        statements: [{ query: "SELECT nuke_materialize();" }],
      });
      machine.send("COMMAND_STARTING_HAS_ROWS");
      expect(machine.state.matches("commandInProgressHasRows")).toBeTruthy();

      const error = {
        message: "Corruption error: All tables and sources have been dropped",
        code: ErrorCode.DATA_CORRUPTED,
      };
      machine.send({
        type: "ERROR",
        error,
      });

      machine.send({
        type: "READY_FOR_QUERY",
      });

      expect(machine.state.context.latestCommandOutput?.error).toEqual(error);
    });

    it("notice in the commandInProgressDefault state", () => {
      const machine = interpret(webSocketFsm);
      machine.start();
      machine.send("READY_FOR_QUERY");
      machine.send({
        type: "SEND",
        command: "DROP mz_internal.memes();",
        statements: [{ query: "DROP mz_internal.memes();" }],
      });
      machine.send("COMMAND_STARTING_DEFAULT");
      expect(machine.state.matches("commandInProgressDefault")).toBeTruthy();

      const notice = {
        message: "executing potentially dangerous functions is not recommended",
        severity: "Notice" as const,
      };

      machine.send({
        type: "NOTICE",
        notice,
      });

      expect(
        machine.state.context.latestCommandOutput?.commandResults[0].notices[0],
      ).toEqual(notice);
    });

    it("error in the commandSent state", () => {
      const machine = interpret(webSocketFsm);
      machine.start();
      machine.send("READY_FOR_QUERY");
      machine.send({
        type: "SEND",
        command: "NOTSQL;",
        statements: [{ query: "NOTSQL;" }],
      });
      expect(machine.state.matches("commandSent")).toBeTruthy();

      const error = {
        message:
          'Error: Expected a keyword at the beginning of a statement, found identifier "notsql"',
        code: ErrorCode.DATA_CORRUPTED,
      };

      machine.send({
        type: "ERROR",
        error,
      });

      expect(machine.state.matches("commandSent")).toBeTruthy();

      expect(machine.state.context.latestCommandOutput?.error).toEqual(error);
    });

    it("notice in the commandSent state", () => {
      const machine = interpret(webSocketFsm);
      machine.start();
      machine.send("READY_FOR_QUERY");
      machine.send({
        type: "SEND",
        command: "SELECT mz_internal.memes();",
        statements: [{ query: "SELECT mz_internal.memes()" }],
      });
      expect(machine.state.matches("commandSent")).toBeTruthy();

      const notice = {
        message: "executing potentially dangerous functions is not recommended",
        severity: "Notice" as const,
      };

      machine.send({
        type: "NOTICE",
        notice,
      });

      expect(
        machine.state.context.latestCommandOutput?.commandResults[0].notices[0],
      ).toEqual(notice);
    });
  });

  it("should merge a notice into a command that results in an error", () => {
    const machine = interpret(webSocketFsm);
    machine.start();
    machine.send("READY_FOR_QUERY");
    machine.send({
      type: "SEND",
      command: "SELECT nuke_materialize();",
      statements: [{ query: "SELECT nuke_materialize();" }],
    });
    machine.send("COMMAND_STARTING_HAS_ROWS");
    expect(machine.state.matches("commandInProgressHasRows")).toBeTruthy();

    const error = {
      message: "Corruption error: All tables and sources have been dropped",
      code: ErrorCode.DATA_CORRUPTED,
    };
    machine.send({
      type: "ERROR",
      error,
    });

    const notice = {
      message: "executing potentially dangerous functions is not recommended",
      severity: "Notice" as const,
    };

    machine.send({
      type: "NOTICE",
      notice,
    });

    machine.send({
      type: "READY_FOR_QUERY",
    });

    expect(
      machine.state.context.latestCommandOutput?.commandResults[0].notices[0],
    ).toEqual(notice);
  });

  it("should still populate rows as an empty array if a query gets a COMMAND_STARTING_HAS_ROWS action but doesn't return any rows ", () => {
    const machine = interpret(webSocketFsm);
    machine.start();
    machine.send("READY_FOR_QUERY");
    machine.send({
      type: "SEND",
      command: "SELECT mz_internal.memes;",
      statements: [{ query: "SELECT mz_internal.memes;" }],
    });

    expect(machine.state.matches("commandSent")).toBeTruthy();

    machine.send({
      type: "COMMAND_STARTING_HAS_ROWS",
    });

    machine.send({
      type: "COMMAND_COMPLETE",
      commandCompletePayload: "SELECT 0;",
    });

    expect(
      machine.state.context.latestCommandOutput?.commandResults[0].rows,
    ).toEqual([]);
  });

  it("should still populate rows as an empty array if a query gets a COMMAND_STARTING_IS_STREAMING action with hasRows = true, but doesn't return any rows ", () => {
    const machine = interpret(webSocketFsm);
    machine.start();
    machine.send("READY_FOR_QUERY");
    machine.send({
      type: "SEND",
      command: "SELECT mz_internal.memes;",
      statements: [{ query: "SELECT mz_internal.memes;" }],
    });

    expect(machine.state.matches("commandSent")).toBeTruthy();

    machine.send({
      type: "COMMAND_STARTING_IS_STREAMING",
      hasRows: true,
    });

    machine.send({
      type: "COMMAND_COMPLETE",
      commandCompletePayload: "SELECT 0;",
    });

    expect(
      machine.state.context.latestCommandOutput?.commandResults[0].rows,
    ).toEqual([]);
  });
});
