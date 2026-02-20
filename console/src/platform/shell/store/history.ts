// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as Sentry from "@sentry/react";
import deepEqual from "fast-deep-equal";
import { atom } from "jotai";
import { atomFamily } from "jotai/utils";

import { promptAtom, PromptValue } from "./prompt";

export interface CommandListEntry {
  id: string;
  command: string;
  label: string;
}

function searchCommand(
  { generation, value, originalValue }: PromptValue,
  searchQuery: string,
): CommandListEntry | null {
  const searchValue = originalValue ?? value;
  if (searchValue === "") return null;
  if (searchQuery === "")
    return {
      id: generation.toString(),
      command: searchValue,
      label: searchValue,
    };
  const firstStartPosition = searchValue.indexOf(searchQuery);
  if (firstStartPosition === -1) return null;
  const leadingSymbol = firstStartPosition === 0 ? "" : "…";
  return {
    id: generation.toString(),
    command: searchValue,
    label: `${leadingSymbol}${searchValue.substring(firstStartPosition)}`,
  };
}

function getPromptCommandReducer(filter: string) {
  return (acc: CommandListEntry[], promptVal: PromptValue) => {
    const commandEntry = searchCommand(promptVal, filter);
    if (commandEntry !== null) {
      acc.push(commandEntry);
    }
    return acc;
  };
}

export const filteredShellHistory = atomFamily(
  (filter: string) =>
    atom((get) => {
      const filterStart = performance.now();

      const prompt = get(promptAtom);
      const reducer = getPromptCommandReducer(filter);
      // Start the filtered list with commands in the future buffer. This will
      // be non-empty if a user has scrubbed back through their history and
      // then launched the history search.
      const commands = prompt.future.reduce(reducer, [] as CommandListEntry[]);
      // If the user has scrubbed back, the current prompt value is _also_
      // historical, and should be searched.
      if (prompt.future.length > 0) {
        const entry = searchCommand(prompt.present, filter);
        if (entry != null) {
          commands.push(entry);
        }
      }
      // Finally, append the past commands from the right to fill out the list
      // in descending recency.
      prompt.past.reduceRight(reducer, commands);

      const filterDurationSecs = (performance.now() - filterStart) / 1000;
      if (filterDurationSecs >= 2) {
        Sentry.captureException(
          new Error("Shell history search long-running filter"),
          {
            extra: {
              numCommands: commands.length,
              filterLength: filter.length,
              filterDurationSecs,
            },
          },
        );
      }

      return commands;
    }),
  deepEqual,
);
