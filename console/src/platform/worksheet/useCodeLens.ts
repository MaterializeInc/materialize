// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useAtomValue } from "jotai";
import * as monaco from "monaco-editor";
import { useEffect, useRef } from "react";

import type { StatementInfo } from "./store";
import { worksheetSessionAtom, worksheetStatementsAtom } from "./store";
import { SHOW_CREATE_KINDS, TEXT_EXPLAIN_KINDS } from "./useExecution";

/** Create statement kinds that target a cluster. */
const CLUSTER_CREATE_KINDS = new Set([
  "create_index",
  "create_materialized_view",
  "create_webhook",
  "create_source",
  "create_sink",
]);

/** Statement kinds that execute against the active cluster. */
const EXECUTE_ON_CLUSTER_KINDS = new Set([
  "select",
  "insert",
  "update",
  "delete",
  "subscribe",
]);

function codeLensTitle(stmt: StatementInfo, sessionCluster: string): string {
  if (SHOW_CREATE_KINDS.has(stmt.kind)) return "\u25B6 Show SQL";
  if (TEXT_EXPLAIN_KINDS.has(stmt.kind)) return "\u25B6 Explain";
  if (CLUSTER_CREATE_KINDS.has(stmt.kind))
    return `\u25B6 Create in ${stmt.inCluster ?? sessionCluster}`;
  if (stmt.kind.startsWith("create_")) return "\u25B6 Create";
  if (EXECUTE_ON_CLUSTER_KINDS.has(stmt.kind))
    return `\u25B6 Execute on ${sessionCluster}`;
  return "\u25B6 Run";
}

/**
 * Registers a Monaco CodeLens provider that places an action label above each
 * parsed statement. Clicking a lens calls `onExecute` with the statement's
 * SQL, kind, and byte offset.
 */
export function useCodeLens(
  editorRef: React.RefObject<monaco.editor.IStandaloneCodeEditor | null>,
  onExecute: (sql: string, kind: string, offset: number) => void,
) {
  const statements = useAtomValue(worksheetStatementsAtom);
  const session = useAtomValue(worksheetSessionAtom);
  const disposableRef = useRef<monaco.IDisposable>();
  const onExecuteRef = useRef(onExecute);
  const statementsRef = useRef(statements);
  const clusterRef = useRef(session.cluster);

  useEffect(() => {
    onExecuteRef.current = onExecute;
  }, [onExecute]);
  useEffect(() => {
    statementsRef.current = statements;
  }, [statements]);
  useEffect(() => {
    clusterRef.current = session.cluster;
  }, [session.cluster]);

  const emitterRef =
    useRef<monaco.Emitter<monaco.languages.CodeLensProvider>>();

  // Register provider once. It reads from refs, so lenses that persist
  // across re-parses are never removed and re-added. The effect re-runs
  // on `statements` changes so it can register once the editor is mounted
  // (first statement change happens after mount), but the guard prevents
  // re-registration. No cleanup is returned here — disposal is handled
  // by the unmount effect below.
  useEffect(() => {
    const editor = editorRef.current;
    if (!editor || disposableRef.current) return;

    const commandId = editor.addCommand(
      0,
      (_ctx, sql: string, kind: string, offset: number) =>
        onExecuteRef.current(sql, kind, offset),
    );
    if (commandId == null) return;

    const emitter = new monaco.Emitter<monaco.languages.CodeLensProvider>();
    emitterRef.current = emitter;

    disposableRef.current = monaco.languages.registerCodeLensProvider("sql", {
      onDidChange: emitter.event,
      provideCodeLenses() {
        return {
          lenses: statementsRef.current.map((stmt) => ({
            range: new monaco.Range(stmt.startLine, 1, stmt.startLine, 1),
            command: {
              id: commandId,
              title: codeLensTitle(stmt, clusterRef.current),
              arguments: [stmt.sql, stmt.kind, stmt.offset],
            },
          })),
          dispose() {},
        };
      },
    });
  }, [editorRef, statements]);

  // Clean up provider and emitter on unmount only.
  useEffect(() => {
    return () => {
      disposableRef.current?.dispose();
      emitterRef.current?.dispose();
    };
  }, []);

  // Signal Monaco to re-query lenses when statements or cluster change.
  useEffect(() => {
    emitterRef.current?.fire(undefined as never);
  }, [statements, session.cluster]);
}
