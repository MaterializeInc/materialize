// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  historyKeymap,
  indentWithTab,
  standardKeymap,
} from "@codemirror/commands";
import { keymap } from "@codemirror/view";

import { isMac } from "~/util";

export const trackedKeyEvents = ["Enter", "ArrowUp", "ArrowDown"] as const;

export const trackedCommands = [
  { key: "g", command: "find" },
  { key: "Enter", command: "submit" },
] as const;

export type EditorCommand =
  | { command: Exclude<(typeof trackedCommands)[number]["command"], "submit"> }
  | { command: "submit"; state: { text: string } };

export type CursorPosition = {
  line: number;
  column: number;
};

export type EditorEvent = {
  key: (typeof trackedKeyEvents)[number];
  state: {
    text: string;
    lineCount: number;
  };
  cursor: CursorPosition;
};

export function keymapHandler(
  onKeyDown?: (event: EditorEvent) => boolean,
  onCommand?: (command: EditorCommand) => boolean,
) {
  return keymap.of([
    {
      any: (view, value) => {
        // Keystroke handler
        if (
          onKeyDown &&
          // Only emit events when a user is not selecting text
          value.shiftKey === false &&
          value.metaKey === false &&
          value.ctrlKey === false &&
          view.state.selection.main.from === view.state.selection.main.to &&
          (trackedKeyEvents as ReadonlyArray<string>).includes(value.key)
        ) {
          const line = view.state.doc.lineAt(view.state.selection.main.head);
          const column = view.state.selection.main.head - line.from;
          return onKeyDown({
            key: value.key as (typeof trackedKeyEvents)[number],
            state: {
              text: view.state.doc.toString(),
              lineCount: view.state.doc.lines,
            },
            cursor: { line: line.number, column },
          });
        }
        // Command handler
        if (
          onCommand &&
          value.shiftKey === false &&
          ((isMac() && value.metaKey === true && value.ctrlKey === false) ||
            (!isMac() && value.ctrlKey === true && value.metaKey === false))
        ) {
          const command = trackedCommands.find((c) => c.key === value.key);
          if (command) {
            return onCommand({
              command: command.command,
              state: {
                text: view.state.doc.toString(),
              },
            });
          }
        }
        return false;
      },
    },
    ...standardKeymap,
    ...historyKeymap,
    indentWithTab,
  ]);
}
