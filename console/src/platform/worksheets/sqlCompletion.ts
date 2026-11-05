// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useLatestRef } from "@chakra-ui/react";
import { CompletionContext, CompletionSource } from "@codemirror/autocomplete";
import {
  keywordCompletionSource,
  schemaCompletionSource,
  SQLNamespace,
} from "@codemirror/lang-sql";
import { LanguageSupport } from "@codemirror/language";
import { Extension } from "@codemirror/state";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useMemo } from "react";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import {
  CompletionCatalogRelation,
  fetchCompletionCatalog,
} from "~/api/materialize/worksheet/completionCatalog";
import { mzDialect } from "~/components/CommandBlock/mzDialect";
import { useAllObjects } from "~/store/allObjects";

/** The database a worksheet session starts in. Cells never change it. */
const SESSION_DATABASE = "materialize";
/**
 * Schemas whose relations resolve unqualified: the default search path, plus
 * `mz_catalog` and `pg_catalog`, which Materialize always searches.
 */
const UNQUALIFIED_SCHEMAS = new Set(["public", "mz_catalog", "pg_catalog"]);

type MutableNamespace = { [name: string]: MutableNamespace | string[] };

function nested(namespace: MutableNamespace, name: string) {
  const existing = namespace[name];
  if (existing && !Array.isArray(existing)) return existing;
  // A relation and a schema can share a name; the schema wins, since a
  // schema prefix is the more common thing to be typing.
  const created: MutableNamespace = {};
  namespace[name] = created;
  return created;
}

function setRelation(
  namespace: MutableNamespace,
  name: string,
  columns: string[],
) {
  if (!(name in namespace)) namespace[name] = columns;
}

/**
 * Builds the completion namespace for lang-sql. Every relation completes by
 * its fully qualified name, by `schema.name` when that resolves from the
 * session database, and by `name` alone when its schema is on the search path.
 */
export function buildCompletionNamespace(
  relations: readonly CompletionCatalogRelation[],
): SQLNamespace {
  const root: MutableNamespace = {};
  for (const { databaseName, schemaName, name, columns } of relations) {
    if (databaseName) {
      setRelation(
        nested(nested(root, databaseName), schemaName),
        name,
        columns,
      );
    }
    if (!databaseName || databaseName === SESSION_DATABASE) {
      setRelation(nested(root, schemaName), name, columns);
      if (UNQUALIFIED_SCHEMAS.has(schemaName)) {
        setRelation(root, name, columns);
      }
    }
  }
  return root;
}

const CELL_REFERENCE_BEFORE = /\{\{\s*[a-z0-9_]*$/;

/** Keywords never follow a `.`, where only a member name can go. */
const AFTER_DOT = /\.[\w$]*$/;

/** Drops a source's results where `skip` matches the text before the cursor. */
function except(source: CompletionSource, skip: RegExp): CompletionSource {
  return (context) => (context.matchBefore(skip) ? null : source(context));
}

/** Completes cell names inside `{{ }}`, closing the braces if needed. */
export function cellReferenceCompletion(
  getCellNames: () => readonly string[],
): CompletionSource {
  return (context: CompletionContext) => {
    const before = context.matchBefore(CELL_REFERENCE_BEFORE);
    if (!before) return null;
    const nameStart = before.from + before.text.search(/[a-z0-9_]*$/);
    const closed =
      context.state.sliceDoc(context.pos, context.pos + 2) === "}}";
    return {
      from: nameStart,
      validFor: /^[a-z0-9_]*$/,
      options: getCellNames().map((name) => ({
        label: name,
        type: "variable",
        detail: "cell",
        apply: closed ? name : `${name}}}`,
      })),
    };
  };
}

const completionQueryKeys = {
  catalog: (version: string) =>
    [
      buildRegionQueryKey("worksheet"),
      buildQueryKeyPart("completionCatalog", { version }),
    ] as const,
};

/** A cheap fingerprint of the catalog's objects, to refetch columns on change. */
function fingerprint(parts: readonly string[]) {
  let hash = 0;
  for (const part of parts) {
    for (let i = 0; i < part.length; i++) {
      hash = (hash * 31 + part.charCodeAt(i)) | 0;
    }
  }
  return `${parts.length}:${hash}`;
}

/**
 * Every relation with its columns. Fetched once and refetched whenever the
 * live catalog subscription reports a different set of objects, so a
 * relation created from a cell appears without a reload.
 */
export function useCompletionCatalog() {
  const { data: objects } = useAllObjects();
  const version = useMemo(
    () => fingerprint(objects.map((o) => `${o.id}:${o.name}`)),
    [objects],
  );
  const { data } = useQuery({
    queryKey: completionQueryKeys.catalog(version),
    queryFn: ({ queryKey, signal }) =>
      fetchCompletionCatalog({ queryKey, requestOptions: { signal } }),
    staleTime: Infinity,
    placeholderData: keepPreviousData,
  });
  return data;
}

/**
 * The SQL language extension for worksheet editors, completing keywords,
 * catalog relations and their columns, and cell references.
 */
export function useWorksheetSqlLanguage(
  cellNames: readonly string[],
): Extension {
  const relations = useCompletionCatalog();
  const cellNamesRef = useLatestRef(cellNames);
  return useMemo(() => {
    // Assembled by hand instead of with `sql()`, so that each source stays
    // out of the places it has nothing useful to offer.
    const schemaSource = schemaCompletionSource({
      dialect: mzDialect,
      schema: buildCompletionNamespace(relations ?? []),
    });
    const keywordSource = keywordCompletionSource(mzDialect);
    return new LanguageSupport(mzDialect.language, [
      mzDialect.language.data.of({
        autocomplete: except(schemaSource, CELL_REFERENCE_BEFORE),
      }),
      mzDialect.language.data.of({
        autocomplete: except(
          except(keywordSource, CELL_REFERENCE_BEFORE),
          AFTER_DOT,
        ),
      }),
      mzDialect.language.data.of({
        autocomplete: cellReferenceCompletion(() => cellNamesRef.current),
      }),
    ]);
  }, [relations, cellNamesRef]);
}
