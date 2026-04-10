// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * @module
 * Registers a Monaco completion provider for Materialize catalog objects.
 *
 * Uses the global `allObjects` subscribe data and the current worksheet
 * session context (database, search_path) to offer context-aware suggestions:
 *
 * - Objects in the current schema: unqualified name (`my_table`)
 * - Objects in another schema of the current database: schema-qualified (`other.my_table`)
 * - Objects in a different database: fully qualified (`db.schema.my_table`)
 *
 * System catalog objects (mz_catalog, mz_internal) are included but sorted
 * after user objects.
 */

import { useAtomValue } from "jotai";
import * as monaco from "monaco-editor";
import React from "react";

import { useAllObjects } from "~/store/allObjects";

import { worksheetSessionAtom } from "./store";

const SYSTEM_SCHEMAS = new Set([
  "mz_catalog",
  "mz_internal",
  "mz_unsafe",
  "pg_catalog",
  "information_schema",
]);

/** Maps object types to Monaco CompletionItemKind for visual distinction in the dropdown. */
function objectTypeToCompletionKind(
  objectType: string,
): monaco.languages.CompletionItemKind {
  switch (objectType) {
    case "table":
      return monaco.languages.CompletionItemKind.Struct;
    case "view":
    case "materialized-view":
      return monaco.languages.CompletionItemKind.Interface;
    case "source":
    case "sink":
      return monaco.languages.CompletionItemKind.Event;
    case "connection":
      return monaco.languages.CompletionItemKind.Module;
    case "secret":
      return monaco.languages.CompletionItemKind.Property;
    default:
      return monaco.languages.CompletionItemKind.Value;
  }
}

/**
 * Registers a Monaco completion provider that suggests catalog object names.
 * Re-registers when the object list or session context changes so suggestions
 * are always current.
 */
export function useObjectCompletions(
  editorRef: React.RefObject<monaco.editor.IStandaloneCodeEditor | null>,
) {
  const { data: objects } = useAllObjects();
  const session = useAtomValue(worksheetSessionAtom);

  // Store current values in refs so the provider callback always reads
  // the latest data without needing to re-register on every change.
  const objectsRef = React.useRef(objects);
  const sessionRef = React.useRef(session);
  React.useEffect(() => {
    objectsRef.current = objects;
  }, [objects]);
  React.useEffect(() => {
    sessionRef.current = session;
  }, [session]);

  React.useEffect(() => {
    const disposable = monaco.languages.registerCompletionItemProvider("sql", {
      triggerCharacters: ["."],
      provideCompletionItems(model, position) {
        // Scan backwards to find any dotted qualifier prefix (e.g. "raw." or "db.schema.")
        const lineContent = model.getLineContent(position.lineNumber);
        const textBeforeCursor = lineContent.substring(0, position.column - 1);
        const qualifiedMatch = textBeforeCursor.match(
          /([\w]+\.)?([\w]+\.)?([\w]*)$/,
        );

        let prefixPart1: string | undefined; // database or schema
        let prefixPart2: string | undefined; // schema (when part1 is database)
        let startColumn = position.column;

        if (qualifiedMatch) {
          const fullMatch = qualifiedMatch[0];
          startColumn = position.column - fullMatch.length;
          prefixPart1 = qualifiedMatch[1]?.replace(".", "");
          prefixPart2 = qualifiedMatch[2]?.replace(".", "");
        }

        const range = new monaco.Range(
          position.lineNumber,
          startColumn,
          position.lineNumber,
          position.column,
        );

        const currentObjects = objectsRef.current;
        const currentSession = sessionRef.current;

        const suggestions: monaco.languages.CompletionItem[] = [];

        for (const obj of currentObjects) {
          if (obj.objectType === "index") continue;

          // If user typed "db.schema.", only show objects in that db+schema
          if (prefixPart1 && prefixPart2) {
            if (
              obj.databaseName !== prefixPart1 ||
              obj.schemaName !== prefixPart2
            ) {
              continue;
            }
          }
          // If user typed "qualifier.", it could be a schema or database
          else if (prefixPart1 && !prefixPart2) {
            const matchesSchema =
              obj.databaseName === currentSession.database &&
              obj.schemaName === prefixPart1;
            const matchesDatabase = obj.databaseName === prefixPart1;
            if (!matchesSchema && !matchesDatabase) continue;
          }

          const inCurrentSchema =
            obj.databaseName === currentSession.database &&
            obj.schemaName === currentSession.searchPath;
          const inCurrentDatabase =
            obj.databaseName === currentSession.database;

          // Determine what to insert — must cover the full range including any typed prefix.
          // Objects with null databaseName (system catalog) are at most schema-qualified.
          let insertText: string;
          let label: string;
          if (prefixPart1 && prefixPart2) {
            // User typed "db.schema." — insert "db.schema.name"
            if (obj.databaseName) {
              insertText = `${obj.databaseName}.${obj.schemaName}.${obj.name}`;
            } else {
              insertText = `${obj.schemaName}.${obj.name}`;
            }
            label = obj.name;
          } else if (prefixPart1) {
            // User typed "qualifier." — insert "qualifier.name"
            if (
              obj.databaseName === currentSession.database &&
              obj.schemaName === prefixPart1
            ) {
              insertText = `${obj.schemaName}.${obj.name}`;
            } else if (obj.databaseName) {
              insertText = `${obj.databaseName}.${obj.schemaName}.${obj.name}`;
            } else {
              insertText = `${obj.schemaName}.${obj.name}`;
            }
            label = obj.name;
          } else if (inCurrentSchema) {
            insertText = obj.name;
            label = obj.name;
          } else if (inCurrentDatabase) {
            insertText = `${obj.schemaName}.${obj.name}`;
            label = `${obj.schemaName}.${obj.name}`;
          } else if (obj.databaseName) {
            insertText = `${obj.databaseName}.${obj.schemaName}.${obj.name}`;
            label = `${obj.databaseName}.${obj.schemaName}.${obj.name}`;
          } else {
            insertText = `${obj.schemaName}.${obj.name}`;
            label = `${obj.schemaName}.${obj.name}`;
          }

          const isSystem = SYSTEM_SCHEMAS.has(obj.schemaName);

          suggestions.push({
            label,
            kind: objectTypeToCompletionKind(obj.objectType),
            insertText,
            filterText: insertText,
            detail: obj.objectType,
            range,
            sortText: isSystem ? `z_${label}` : `a_${label}`,
          });
        }

        return { suggestions };
      },
    });

    return () => disposable.dispose();
  }, []); // Register once — refs keep data current
}
