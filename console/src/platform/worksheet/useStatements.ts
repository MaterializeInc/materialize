// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO: Switch to @materializeinc/sql-parser when published.
import { parse } from "@sjwiesman/sql-parser";
import { useSetAtom } from "jotai";
import type { editor } from "monaco-editor";
import { useCallback, useEffect, useRef } from "react";

import { PARSE_DEBOUNCE_MS } from "./constants";
import {
  type StatementInfo,
  worksheetParseErrorAtom,
  worksheetStatementsAtom,
} from "./store";

/**
 * Parses the editor content with the WASM SQL parser on every change (debounced).
 * Updates `worksheetStatementsAtom` with successfully parsed statements and
 * `worksheetParseErrorAtom` with the first parse error, if any.
 *
 * When the input contains a mix of valid and invalid statements, both atoms
 * are populated — valid statements still get CodeLens actions.
 */
export function useStatements() {
  const setStatements = useSetAtom(worksheetStatementsAtom);
  const setParseError = useSetAtom(worksheetParseErrorAtom);
  const timerRef = useRef<ReturnType<typeof setTimeout>>();

  // Clear any pending debounce timer on unmount to avoid firing on a disposed Monaco model.
  useEffect(() => {
    return () => clearTimeout(timerRef.current);
  }, []);

  const parseContent = useCallback(
    (model: editor.ITextModel) => {
      clearTimeout(timerRef.current);
      timerRef.current = setTimeout(() => {
        const text = model.getValue();
        const result = parse(text);

        // Update error state
        if (result.error) {
          const pos = model.getPositionAt(result.error.offset);
          const endPos = model.getPositionAt(
            Math.min(result.error.offset + 1, text.length),
          );
          setParseError({
            message: result.error.message,
            offset: result.error.offset,
            startLine: pos.lineNumber,
            startColumn: pos.column,
            endLine: endPos.lineNumber,
            endColumn: endPos.column,
          });
        } else {
          setParseError(null);
        }

        // Update statements — the parser now returns partial results
        // even when there's an error, so we always process them.
        if (result.statements.length > 0) {
          const stmts: StatementInfo[] = result.statements.map((s) => {
            const startPos = model.getPositionAt(s.offset);
            const endPos = model.getPositionAt(s.offset + s.sql.length);
            return {
              kind: s.kind,
              sql: s.sql,
              offset: s.offset,
              inCluster: s.in_cluster ?? null,
              startLine: startPos.lineNumber,
              startColumn: startPos.column,
              endLine: endPos.lineNumber,
              endColumn: endPos.column,
            };
          });
          setStatements(stmts);
        } else {
          setStatements([]);
        }
      }, PARSE_DEBOUNCE_MS);
    },
    [setStatements, setParseError],
  );

  return { parseContent };
}
