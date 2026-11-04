// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { EditorView } from "@codemirror/view";

// Clicking the explorer moves focus away from the editor, so the target is
// the editor focused last, not the one focused now.
let lastFocused: EditorView | undefined;
let fallback: EditorView | undefined;

export function noteEditorFocused(view: EditorView) {
  lastFocused = view;
}

/** Sets the editor that receives text when no editor has been focused. */
export function setFallbackEditor(view: EditorView | undefined) {
  fallback = view;
}

export function forgetEditor(view: EditorView) {
  if (lastFocused === view) lastFocused = undefined;
  if (fallback === view) fallback = undefined;
}

/** Inserts `text` at the cursor of the editor focused last, and focuses it. */
export function insertIntoActiveEditor(text: string) {
  const view = lastFocused?.dom.isConnected ? lastFocused : fallback;
  if (!view) return;
  const { state } = view;
  const before = state.sliceDoc(
    state.selection.main.from - 1,
    state.selection.main.from,
  );
  // Separate the name from a preceding word, but not from `(` or `.`.
  const prefix = before && /[\w"')]/.test(before) ? " " : "";
  view.dispatch(state.replaceSelection(prefix + text));
  view.focus();
}
