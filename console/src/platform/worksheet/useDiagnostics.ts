// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useAtomValue } from "jotai";
import { useMonaco } from "@monaco-editor/react";
import type * as monacoEditor from "monaco-editor";
import { useEffect } from "react";

import { worksheetParseErrorAtom } from "./store";

/**
 * Syncs parse errors from `worksheetParseErrorAtom` to Monaco editor markers.
 * Shows red squiggles on the error position when a parse error exists,
 * and clears them when the error is resolved.
 */
export function useDiagnostics(
  editorRef: React.RefObject<monacoEditor.editor.IStandaloneCodeEditor | null>,
) {
  const parseError = useAtomValue(worksheetParseErrorAtom);
  const monaco = useMonaco();

  useEffect(() => {
    const editor = editorRef.current;
    if (!editor) return;
    if (!monaco) return;
    const model = editor.getModel();
    if (!model) return;

    if (parseError) {
      monaco.editor.setModelMarkers(model, "mz-sql-parser", [
        {
          severity: monaco.MarkerSeverity.Error,
          message: parseError.message,
          startLineNumber: parseError.startLine,
          startColumn: parseError.startColumn,
          endLineNumber: parseError.endLine,
          endColumn: parseError.endColumn,
        },
      ]);
    } else {
      monaco.editor.setModelMarkers(model, "mz-sql-parser", []);
    }
  }, [editorRef, parseError, monaco]);
}
