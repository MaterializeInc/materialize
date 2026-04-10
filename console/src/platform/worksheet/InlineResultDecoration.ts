// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useAtomValue } from "jotai";
import type { editor } from "monaco-editor";
import { useEffect, useRef } from "react";

import { worksheetInlineResultsAtom, worksheetStatementsAtom } from "./store";

/**
 * Manages Monaco ViewZone decorations that show inline result tags
 * (e.g., "INSERT 0 1" or error messages) below the corresponding statement.
 */
export function useInlineResultDecorations(
  editorRef: React.RefObject<editor.IStandaloneCodeEditor | null>,
) {
  const inlineResults = useAtomValue(worksheetInlineResultsAtom);
  const statements = useAtomValue(worksheetStatementsAtom);
  const zoneIdsRef = useRef<string[]>([]);

  useEffect(() => {
    const ed = editorRef.current;
    if (!ed) return;

    ed.changeViewZones((accessor) => {
      for (const id of zoneIdsRef.current) {
        accessor.removeZone(id);
      }
      zoneIdsRef.current = [];

      for (const [offset, result] of inlineResults) {
        const stmt = statements.find((s) => s.offset === offset);
        if (!stmt) continue;

        const isError = result.kind === "error";

        const domNode = document.createElement("div");
        domNode.style.fontSize = "12px";
        domNode.style.paddingLeft = "16px";
        domNode.style.fontFamily = "Roboto Mono, monospace";
        domNode.style.color = isError ? "#f44747" : "#6a9955";
        domNode.textContent = isError
          ? `\u2717 ${result.message}`
          : `\u2713 ${result.message}`;

        const id = accessor.addZone({
          afterLineNumber: stmt.endLine,
          heightInPx: 20,
          domNode,
        });
        zoneIdsRef.current.push(id);
      }
    });
  }, [editorRef, inlineResults, statements]);
}
