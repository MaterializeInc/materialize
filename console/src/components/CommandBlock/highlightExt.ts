// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { StateEffect, StateField } from "@codemirror/state";
import { Decoration, EditorView } from "@codemirror/view";

import { escapeRegExp } from "~/util";

export const setHighlight = StateEffect.define<string>();

export const highlightMark = Decoration.mark({ class: "highlight" });

const highlightExt = StateField.define({
  create() {
    return Decoration.none;
  },
  update(decorations, transaction) {
    decorations = decorations.map(transaction.changes);
    const highlights = [];
    for (const effect of transaction.effects) {
      if (!effect.is(setHighlight)) {
        continue;
      }
      const query = effect.value;
      if (query === "") {
        continue;
      }
      const pattern = new RegExp(escapeRegExp(query), "g");
      const cursor = transaction.startState.doc.iter();
      let position = 0;
      while (!cursor.next().done) {
        const line = cursor.value;
        let match;
        while ((match = pattern.exec(line))) {
          const from = position + match.index;
          const to = from + match[0].length;
          if (from - to === 0) {
            // We've got an empty match, bail on this line.
            break;
          }
          highlights.push(highlightMark.range(from, to));
        }
        position += line.length;
      }
    }
    decorations = decorations.update({
      add: highlights,
      // Clear the pre-existing highlights
      filter: () => false,
    });
    return decorations;
  },
  provide: (facet) => EditorView.decorations.from(facet),
});

export default highlightExt;
