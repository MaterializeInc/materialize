// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { sql } from "kysely";

import {
  buildFullyQualifiedObjectName,
  escapedLiteral as lit,
  executeSqlV2,
  queryBuilder,
} from "~/api/materialize";
import { isDate, isNumber, isObject } from "~/util";

import { Source } from "./sourceList";

type ColumnDef = [
  // name
  string | number,
  // wrapping function
  string,
  // cast type name
  string,
  // parents
  (string | number)[],
  // is null?
  boolean,
  // value
  unknown,
];

/* JSON Parsing and SQL conversion */

/**
 * Extracts all keys from a JSON object into a list of fields, and their chain
 * of parents.
 * @param json - a sample object string
 * @param columnName - the name of the source SQL column containing the JSON
 * @returns the columns to select
 */
export function extractKeyPaths(json: object, columnName: string = "body") {
  if (!columnName) {
    columnName = "body";
  }

  const selectItems: ColumnDef[] = [];

  expandObject(json, [columnName], selectItems);

  return selectItems;
}

/**
 * Recursively iterates through the provided object, tracking the chain
 * of parent fields for later use in naming and desctructuring.
 */
function expandObject(
  object: unknown,
  parents: string[],
  columns: ColumnDef[],
) {
  if (Array.isArray(object)) {
    handleArray(object, parents, columns);
  } else if (isObject(object)) {
    handleObject(object, parents, columns);
  } else {
    handlePrimitive(object, parents, columns);
  }
}

/** Handles arrays within the JSON structure, including empty arrays. */
function handleArray(
  array: unknown[],
  parents: string[],
  columns: ColumnDef[],
) {
  if (array.length === 0) {
    // Specifically handles empty arrays to add placeholder values rather than skipping them.
    handleEmptyArray(parents, columns);
  } else {
    // For non-empty arrays, iterate over each item, expanding it.
    array.forEach((item, index) => {
      const newParents = parents.concat(index.toString());
      expandObject(item, newParents, columns);
    });
  }
}

/** Handles objects by iterating over each property and recursively expanding it. */
function handleObject(obj: object, parents: string[], columns: ColumnDef[]) {
  Object.entries(obj).forEach(([key, value]) => {
    const newParents = parents.concat(key);
    expandObject(value, newParents, columns);
  });
}

/** Handles primitive values by determining their SQL data type and adding them to the columns list. */
function handlePrimitive(
  value: unknown,
  parents: string[],
  columns: ColumnDef[],
) {
  // Determine the appropriate SQL cast and function wrapper for the value.
  const { cast, wrapping_function, isNull } = determineCast(value);
  // Uses the last part of the path as the column name.
  const columnName = parents.slice(-1)[0];
  // Add the column definition to the list of columns.
  columns.push([columnName, wrapping_function, cast, parents, isNull, value]);
}

/** Handles the specific case of an empty array by adding a placeholder column definition. */
function handleEmptyArray(parents: string[], columns: ColumnDef[]) {
  // Generates a column name and alias based on the path to the empty array.
  const columnAlias = parents.slice(1).join("_");
  // Adds a placeholder column for the empty array.
  columns.push([columnAlias, "", "text", parents, true, "''"]);
}

/**
 * Generate SQL from the columns.
 *
 * @param selectItems - the columns to serialize.
 * @param viewName - the desired name of the generated view.
 * @param sourceName - the name of the webhook source.
 * @param objectType - the type of view to generate.
 * @param columnName - the name of the source column.
 * @returns a SQL string
 */
export function createWebhookSourceViewStatement(
  selectItems: ColumnDef[],
  viewName: string,
  source: Source,
  objectType: "view" | "materialized-view",
  columnName: string = "body",
) {
  let type = "VIEW";
  if (objectType === "materialized-view") {
    type = "MATERIALIZED VIEW";
  }

  const fullyQualifiedViewName = buildFullyQualifiedObjectName({
    ...source,
    name: viewName,
  });

  const sqlParts = selectItems.map(
    ([name, wrapping_function, cast, parents, _isNull, _value]) => {
      let path = sql`${sql.ref(columnName)}`;

      // Iterate over parents to build the path, skipping the columnName/base if it's included
      parents
        .slice(columnName ? 1 : 0, parents.length - 1)
        .forEach((parent) => {
          path = !isNumber(parent)
            ? sql`${path}->${lit(parent)}`
            : sql`${path}->${sql.raw(parent.toString())}`;
        });

      // Determine the final part of the path
      const finalPart = !isNumber(name)
        ? sql`${path}->>${lit(name)}`
        : sql`${path}->${sql.raw(name.toString())}`;

      // Wrap the final part in parentheses if casting is applied
      let sqlItem = cast
        ? sql`(${finalPart})::${sql.raw(cast)}`
        : sql`${finalPart}`;
      if (wrapping_function !== "") {
        sqlItem = sql`${sql.raw(wrapping_function)}(${sqlItem})`;
      }

      // Construct column alias without redundant name appending
      const columnAliasParts = parents.slice(columnName ? 1 : 0);
      const columnAlias = columnAliasParts.join("_");

      return sql`${sqlItem} AS ${sql.id(columnAlias)}`;
    },
  );
  const joined = sql.join(sqlParts, sql`,\n    `);

  return sql`CREATE ${sql.raw(type)} ${fullyQualifiedViewName} AS
  SELECT
    ${joined}
  FROM
    ${buildFullyQualifiedObjectName(source)};`;
}

/** Helper to determine cast and wrapping function based on value type */
function determineCast(value: unknown | null) {
  let cast = "";
  let wrapping_function = "";
  const isNull = value === null;

  if (!isNull) {
    if (typeof value === "boolean") {
      cast = "bool";
    } else if (typeof value === "number") {
      cast = "numeric";
    } else if (typeof value === "string") {
      if (isDate(value)) {
        // If the string is a valid ISO8601 date, cast it to a timestamp
        // TODO: This could be improved to handle other date formats
        wrapping_function = "try_parse_monotonic_iso8601_timestamp";
      }
    }
  }
  return { cast, wrapping_function, isNull };
}

export type CreateWebhookSourceViewParams = {
  sampleObject: object;
  viewName: string;
  source: Source;
};

const createWebhookSourceView = ({
  params,
  queryKey,
  requestOptions,
}: {
  params: CreateWebhookSourceViewParams;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) => {
  const { sampleObject, source, viewName } = params;
  const columns = extractKeyPaths(sampleObject);
  const createView = createWebhookSourceViewStatement(
    columns,
    viewName,
    source,
    "view",
  ).compile(queryBuilder);
  return executeSqlV2({
    queries: [createView],
    queryKey,
    requestOptions,
  });
};

export default createWebhookSourceView;
