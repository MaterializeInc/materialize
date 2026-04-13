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
 * Adds a "See Details" context menu action and Cmd+D hotkey to the worksheet
 * editor. When triggered, resolves the identifier under the cursor to a catalog
 * object and opens its detail view in the catalog sidebar.
 *
 * Handles unqualified (`my_table`), schema-qualified (`raw.my_table`), and
 * fully qualified (`db.raw.my_table`) identifiers by matching against the
 * global `allObjects` data and the current session context.
 */

import { useAtomValue, useSetAtom } from "jotai";
import type { Monaco } from "@monaco-editor/react";
import type * as monacoEditor from "monaco-editor";
import React from "react";

import { parseSearchPath } from "~/api/materialize";
import type { DatabaseObject } from "~/api/materialize/objects";
import type { SupportedObjectType } from "~/api/materialize/types";
import { allObjects } from "~/store/allObjects";
import { catalogDetailAtom, catalogVisibleAtom } from "~/store/catalog";

import { worksheetSessionAtom } from "./store";

/**
 * Extracts a possibly dotted identifier around the cursor position.
 * Returns the full text (e.g. "raw.customers").
 */
export function getQualifiedIdentifier(
  model: monacoEditor.editor.ITextModel,
  position: monacoEditor.Position,
): { text: string } {
  const lineContent = model.getLineContent(position.lineNumber);
  const col = position.column - 1; // 0-based

  // Scan left to find the start of the dotted identifier
  let start = col;
  while (start > 0 && /[\w.]/.test(lineContent[start - 1])) {
    start--;
  }

  // Scan right to find the end
  let end = col;
  while (end < lineContent.length && /[\w.]/.test(lineContent[end])) {
    end++;
  }

  // Trim leading/trailing dots
  let text = lineContent.substring(start, end);
  while (text.startsWith(".")) {
    text = text.substring(1);
  }
  while (text.endsWith(".")) {
    text = text.substring(0, text.length - 1);
  }

  return { text };
}

/**
 * Resolves a possibly qualified identifier to a DatabaseObject.
 * Tries in order:
 * 1. Fully qualified: db.schema.name
 * 2. Schema-qualified: schema.name (using session database)
 * 3. Unqualified: name (using session database + searchPath)
 */
export function resolveObject(
  identifier: string,
  objects: DatabaseObject[],
  session: { database: string; searchPath: string },
): DatabaseObject | undefined {
  const parts = identifier.split(".").map((p) => p.toLowerCase());

  if (parts.length === 3) {
    const [db, schema, name] = parts;
    return objects.find(
      (o) =>
        o.databaseName?.toLowerCase() === db &&
        o.schemaName.toLowerCase() === schema &&
        o.name.toLowerCase() === name,
    );
  }

  if (parts.length === 2) {
    const [schema, name] = parts;
    const dbLower = session.database.toLowerCase();
    // Try current database first, then null-database (system catalog) objects
    return (
      objects.find(
        (o) =>
          o.databaseName?.toLowerCase() === dbLower &&
          o.schemaName.toLowerCase() === schema &&
          o.name.toLowerCase() === name,
      ) ??
      objects.find(
        (o) =>
          o.databaseName === null &&
          o.schemaName.toLowerCase() === schema &&
          o.name.toLowerCase() === name,
      )
    );
  }

  if (parts.length === 1) {
    const [name] = parts;
    const dbLower = session.database.toLowerCase();
    // Resolve via search_path schemas, plus implicit mz_catalog and pg_catalog
    const schemas = [
      ...parseSearchPath(session.searchPath),
      "mz_catalog",
      "pg_catalog",
    ];
    for (const schema of schemas) {
      const schemaLower = schema.toLowerCase();
      const match =
        objects.find(
          (o) =>
            o.databaseName?.toLowerCase() === dbLower &&
            o.schemaName.toLowerCase() === schemaLower &&
            o.name.toLowerCase() === name,
        ) ??
        objects.find(
          (o) =>
            o.databaseName === null &&
            o.schemaName.toLowerCase() === schemaLower &&
            o.name.toLowerCase() === name,
        );
      if (match) return match;
    }
    return undefined;
  }

  return undefined;
}

/**
 * Returns a ref to a function that registers the "See Details" context menu
 * action and Cmd+D keybinding on a Monaco editor instance. Call the ref's
 * current value in the editor's onMount callback, passing both the editor
 * and the Monaco instance.
 *
 * Uses refs internally so the action callback always reads the latest
 * catalog data and session context without re-registering.
 */
export function useGoToDefinition(): React.MutableRefObject<
  (editor: monacoEditor.editor.IStandaloneCodeEditor, monacoInstance: Monaco) => void
> {
  const objectsState = useAtomValue(allObjects);
  const session = useAtomValue(worksheetSessionAtom);
  const setCatalogDetail = useSetAtom(catalogDetailAtom);
  const setCatalogVisible = useSetAtom(catalogVisibleAtom);

  const objectsRef = React.useRef(objectsState.data);
  const sessionRef = React.useRef(session);
  React.useEffect(() => {
    objectsRef.current = objectsState.data;
  }, [objectsState.data]);
  React.useEffect(() => {
    sessionRef.current = session;
  }, [session]);

  const setCatalogDetailRef = React.useRef(setCatalogDetail);
  const setCatalogVisibleRef = React.useRef(setCatalogVisible);
  React.useEffect(() => {
    setCatalogDetailRef.current = setCatalogDetail;
    setCatalogVisibleRef.current = setCatalogVisible;
  }, [setCatalogDetail, setCatalogVisible]);

  const registerRef = React.useRef(
    (editor: monacoEditor.editor.IStandaloneCodeEditor, monacoInstance: Monaco) => {
      editor.addAction({
        id: "worksheet.seeDetails",
        label: "See Details",
        contextMenuGroupId: "navigation",
        contextMenuOrder: 1,
        keybindings: [monacoInstance.KeyMod.CtrlCmd | monacoInstance.KeyCode.KeyD],
        run: (ed) => {
          const position = ed.getPosition();
          if (!position) return;
          const model = ed.getModel();
          if (!model) return;

          const { text } = getQualifiedIdentifier(model, position);
          if (!text) return;

          const obj = resolveObject(
            text,
            objectsRef.current,
            sessionRef.current,
          );
          if (obj) {
            setCatalogDetailRef.current({
              id: obj.id,
              databaseName: obj.databaseName ?? "",
              schemaName: obj.schemaName,
              objectName: obj.name,
              objectType: obj.objectType as SupportedObjectType,
              clusterId: obj.clusterId ?? undefined,
              clusterName: obj.clusterName ?? undefined,
            });
            setCatalogVisibleRef.current(true);
          }
        },
      });
    },
  );

  return registerRef;
}
