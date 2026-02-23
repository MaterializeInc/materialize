// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CommandOutput } from "./store/shell";

export function getCommandResultCompletedTime(
  commandOutput: CommandOutput,
  commandResultIndex: number,
): number | null {
  const { commandResults } = commandOutput;
  const commandResult = commandResults[commandResultIndex];
  if (!commandResult || !commandResult.endTimeMs) {
    return null;
  }
  return commandResult.endTimeMs;
}

/**
 * Given a command result and its corresponding output, determine the number of milliseconds
 * spanned by the query. If the command is not yet done, do not calculate a duration.
 */
export function calculateCommandDuration(
  commandResultIndex: number,
  commandOutput: CommandOutput,
): number | null {
  const commandResultCompletedTime = getCommandResultCompletedTime(
    commandOutput,
    commandResultIndex,
  );

  if (!commandResultCompletedTime) {
    return null;
  }

  const lastCommandResultCompletedTime =
    getCommandResultCompletedTime(commandOutput, commandResultIndex - 1) ??
    commandOutput.commandSentTimeMs;

  if (!lastCommandResultCompletedTime) {
    return null;
  }

  return commandResultCompletedTime - lastCommandResultCompletedTime;
}

/**
 * Provide a human-readable representation of a duration.
 *
 * @param durationMs - the command duration, in milliseconds
 * @returns a string representation of the command duration, with a unit
 */
export function formatCommandDuration(durationMs: number): string {
  let unit = "ms";
  let duration = durationMs.toFixed(1);
  if (durationMs > 1_000) {
    duration = (durationMs / 1000).toFixed(2);
    unit = "s";
  }
  return `${duration}${unit}`;
}

/**
 *
 * @param commandStartTimeMs - the time at which the command started
 * @param timeTakenMs - the time taken by the command, if it has finished
 * @param timeoutMs - the maximum time the command should take
 * @returns whether the command has exceeded the timeout
 */
export function doesCommandExceedTimeout(
  timeoutMs: number,
  commandStartTimeMs: number,
  timeTakenMs: number | null,
) {
  const hasCommandFinished = timeTakenMs !== null;

  const finishedCommandExceedsTimeout =
    hasCommandFinished && timeTakenMs >= timeoutMs;

  const commandInProgressExceedsTimeout =
    !hasCommandFinished && performance.now() >= commandStartTimeMs + timeoutMs;

  return finishedCommandExceedsTimeout || commandInProgressExceedsTimeout;
}
