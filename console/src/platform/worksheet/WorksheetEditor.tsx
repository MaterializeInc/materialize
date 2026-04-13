// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useColorMode } from "@chakra-ui/react";
import { getKeywords } from "@materializeinc/sql-lexer";
import Editor, { type Monaco, type OnMount } from "@monaco-editor/react";
// TODO: Switch to @materializeinc/sql-parser when published.
import { parse } from "@sjwiesman/sql-parser";
import { useAtom, useAtomValue } from "jotai";
import type * as monacoEditor from "monaco-editor";
import React, { useCallback, useEffect, useRef } from "react";

import { SAVE_DEBOUNCE_MS } from "./constants";
import { useInlineResultDecorations } from "./InlineResultDecoration";
import {
  resultsPanelHeightAtom,
  worksheetContentAtom,
  worksheetStatementsAtom,
} from "./store";
import { useCodeLens } from "./useCodeLens";
import { useDiagnostics } from "./useDiagnostics";
import { useGoToDefinition } from "./useGoToDefinition";
import { useObjectCompletions } from "./useObjectCompletions";
import { useStatements } from "./useStatements";

export interface WorksheetEditorProps {
  /** Called when the user executes a statement via CodeLens or Ctrl+Enter. */
  onExecute: (sql: string, kind: string, offset: number) => void;
}

export interface WorksheetEditorHandle {
  /** Replaces editor content with the given SQL, then executes the first statement. */
  insertAndExecute: (sql: string) => void;
}

let languageRegistered = false;

/** Registers the Materialize SQL dialect with Monaco (syntax highlighting + keyword completions). */
function registerSqlLanguage(monacoInstance: Monaco) {
  if (languageRegistered) return;
  languageRegistered = true;

  const keywords = getKeywords();

  monacoInstance.languages.setMonarchTokensProvider("sql", {
    ignoreCase: true,
    keywords: keywords.map((k: string) => k.toLowerCase()),
    tokenizer: {
      root: [
        [/--.*$/, "comment"],
        [/\/\*/, "comment", "@comment"],
        [/'[^']*'/, "string"],
        [/\d+(\.\d+)?/, "number"],
        [
          /[a-zA-Z_]\w*/,
          {
            cases: {
              "@keywords": "keyword",
              "@default": "identifier",
            },
          },
        ],
      ],
      comment: [
        [/\*\//, "comment", "@pop"],
        [/./, "comment"],
      ],
    },
  } as monacoEditor.languages.IMonarchLanguage);

  monacoInstance.languages.registerCompletionItemProvider("sql", {
    provideCompletionItems(model: monacoEditor.editor.ITextModel, position: monacoEditor.Position) {
      const word = model.getWordUntilPosition(position);
      const range = new monacoInstance.Range(
        position.lineNumber,
        word.startColumn,
        position.lineNumber,
        word.endColumn,
      );
      return {
        suggestions: keywords.map((kw: string) => ({
          label: kw.toUpperCase(),
          kind: monacoInstance.languages.CompletionItemKind.Keyword,
          insertText: kw.toUpperCase(),
          range,
        })),
      };
    },
  });
}

const WorksheetEditor = React.forwardRef<
  WorksheetEditorHandle,
  WorksheetEditorProps
>(({ onExecute }, ref) => {
  const { colorMode } = useColorMode();
  const editorRef = useRef<monacoEditor.editor.IStandaloneCodeEditor | null>(null);
  const [content, setContent] = useAtom(worksheetContentAtom);
  const saveTimerRef = useRef<ReturnType<typeof setTimeout>>();
  const { parseContent } = useStatements();
  const statements = useAtomValue(worksheetStatementsAtom);
  const statementsRef = useRef(statements);

  useEffect(() => {
    statementsRef.current = statements;
  }, [statements]);

  // Clean up save timer on unmount
  useEffect(() => {
    return () => clearTimeout(saveTimerRef.current);
  }, []);

  React.useImperativeHandle(
    ref,
    () => ({
      insertAndExecute: (sql: string) => {
        const editor = editorRef.current;
        if (!editor) return;
        const model = editor.getModel();
        if (!model) return;
        model.setValue(sql);
        // Parse synchronously since the debounced parse hasn't fired yet
        const result = parse(sql);
        if (result.statements.length > 0) {
          const stmt = result.statements[0];
          onExecute(stmt.sql, stmt.kind, stmt.offset);
        }
      },
    }),
    [onExecute],
  );

  useCodeLens(editorRef, onExecute);
  useDiagnostics(editorRef);
  useInlineResultDecorations(editorRef);
  useObjectCompletions(editorRef);

  // Add bottom padding to the editor when the result panel is visible
  // so text near the bottom isn't hidden behind the overlay.
  const resultsPanelHeight = useAtomValue(resultsPanelHeightAtom);
  useEffect(() => {
    editorRef.current?.updateOptions({
      padding: { bottom: resultsPanelHeight, top: 0 },
    });
  }, [resultsPanelHeight]);
  const goToDefinitionRef = useGoToDefinition();

  const handleMount: OnMount = useCallback(
    (editor, monaco) => {
      editorRef.current = editor;
      registerSqlLanguage(monaco);
      goToDefinitionRef.current(editor, monaco);

      const model = editor.getModel();
      if (model) {
        parseContent(model);
      }

      // Ctrl+Enter: execute the statement at the cursor position
      editor.addAction({
        id: "worksheet.executeCurrent",
        label: "Execute Current Statement",
        keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter] as const,
        run: (ed) => {
          const position = ed.getPosition();
          if (!position) return;
          const edModel = ed.getModel();
          if (!edModel) return;
          const cursorOffset = edModel.getOffsetAt(position);
          const stmts = statementsRef.current;
          const stmt = stmts.find((s, i) => {
            const nextOffset =
              i < stmts.length - 1
                ? stmts[i + 1].offset
                : edModel.getValue().length;
            return cursorOffset >= s.offset && cursorOffset < nextOffset;
          });
          if (stmt) {
            onExecute(stmt.sql, stmt.kind, stmt.offset);
          }
        },
      });
    },
    // goToDefinitionRef is a useRef return — its identity is stable and will
    // never cause this callback to be recreated.
    [parseContent, onExecute, goToDefinitionRef],
  );

  const handleChange = useCallback(
    (value: string | undefined) => {
      const editor = editorRef.current;
      if (!editor) return;
      const model = editor.getModel();
      if (model) {
        parseContent(model);
      }
      clearTimeout(saveTimerRef.current);
      saveTimerRef.current = setTimeout(() => {
        setContent(value ?? "");
      }, SAVE_DEBOUNCE_MS);
    },
    [parseContent, setContent],
  );

  return (
    <Editor
      defaultLanguage="sql"
      defaultValue={content}
      theme={colorMode === "dark" ? "vs-dark" : "vs"}
      onChange={handleChange}
      onMount={handleMount}
      options={{
        minimap: { enabled: false },
        lineNumbers: "on",
        scrollBeyondLastLine: false,
        fontSize: 14,
        fontFamily: "Roboto Mono, monospace",
        codeLens: true,
        automaticLayout: true,
        tabSize: 2,
        wordWrap: "on",
      }}
    />
  );
});

export default WorksheetEditor;
