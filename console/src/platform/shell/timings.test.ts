// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CommandOutput, CommandResult } from "./store/shell";
import { calculateCommandDuration, getCommandResultStartTime } from "./timings";

const COMMAND_SENT_TIME_MS = 1000;

function commandOutput(commandResults: CommandResult[]): CommandOutput {
  return {
    kind: "command",
    historyId: "1",
    command: "SELECT 1; SELECT 2;",
    statements: [{ query: "SELECT 1" }, { query: "SELECT 2" }],
    commandSentTimeMs: COMMAND_SENT_TIME_MS,
    commandResults,
    commandResultsDisplayStates: [],
  };
}

describe("getCommandResultStartTime", () => {
  it("measures the first result from sending the command", () => {
    const output = commandOutput([{ notices: [], endTimeMs: 1100 }]);

    expect(getCommandResultStartTime(output, 0)).toEqual({
      startTimeMs: COMMAND_SENT_TIME_MS,
      isMeasuredFromPreviousResult: false,
    });
  });

  it("measures a later result from the previous result's completion", () => {
    const output = commandOutput([
      { notices: [], endTimeMs: 1100 },
      { notices: [], endTimeMs: 1104 },
    ]);

    expect(getCommandResultStartTime(output, 1)).toEqual({
      startTimeMs: 1100,
      isMeasuredFromPreviousResult: true,
    });
    expect(calculateCommandDuration(1, output)).toBe(4);
  });

  it("falls back to sending the command while the previous result is incomplete", () => {
    const output = commandOutput([{ notices: [] }, { notices: [] }]);

    expect(getCommandResultStartTime(output, 1)).toEqual({
      startTimeMs: COMMAND_SENT_TIME_MS,
      isMeasuredFromPreviousResult: false,
    });
  });
});
