// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Extension, Transaction } from "@codemirror/state";
import { ViewPlugin } from "@codemirror/view";

import { isSafari } from "~/util";

export const enableSafariCopyPasteHackExtension = isSafari();

/**
 * Note (Jun): In Safari there's a bug where if you have a mouse plugged in and paste a really long piece of text, then the input
 * will overflow vertically such that one cannot read what they're typing (https://github.com/MaterializeInc/console/issues/1087).
 *
 * This is an issue with CodeMirror itself and I've filed an issue for it https://github.com/codemirror/dev/issues/1294.
 *
 * For now, the vertical overflow disappears when you type into the input. The hack here is we listen
 * to paste events, then add and remove a " " character at the very start of the input such that the user doesn't notice.
 *
 * Once the core issue gets solved, we should remove this hack.
 *
 */
function safariCopyPasteHackExtension(): Extension {
  const viewPlugin = ViewPlugin.define((view) => {
    return {
      update: (viewUpdate) => {
        for (const transaction of viewUpdate.transactions) {
          if (transaction.isUserEvent("input.paste")) {
            /* We need a setTimeout since otherwise, dispatches within a running dispatch become discarded */
            setTimeout(() => {
              // It's quite hacky to be dispatching within the plugin itself. Rather, CommandBlock should listen for
              // this event, wait until CodeMirror is ready to dispatch again, then dispatch these events.
              // However, this would require coupling the hack to React's lifecycle.
              // By doing it this way, we reduce complexity and keep the hack isolated from the rest of our business logic.
              const addSpaceAtBeginning = { from: 0, insert: " " };
              const removeSpaceAtBeginning = { from: 0, to: 1 };
              view.dispatch({
                changes: addSpaceAtBeginning,
                annotations: Transaction.addToHistory.of(false),
              });
              view.dispatch({
                changes: removeSpaceAtBeginning,
                annotations: Transaction.addToHistory.of(false),
              });
            }, 50);
          }
        }
      },
    };
  });

  return viewPlugin;
}

export default safariCopyPasteHackExtension;
