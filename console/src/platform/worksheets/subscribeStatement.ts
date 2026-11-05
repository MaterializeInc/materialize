// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { matchParen, stripLeadingTrivia, trimStatement } from "./sqlTokens";

export type CellQuery =
  | { query: string; isSubscribe: boolean; error?: undefined }
  | { query?: undefined; isSubscribe: true; error: string };

const SUBSCRIBE_PREFIX_REGEX = /^subscribe\s+(?:to\s+)?/i;
const QUERY_KEYWORD_REGEX = /^(select|with|values|table)\b/i;
const CELL_REFERENCE_REGEX = /^\{\{\s*[a-z_][a-z0-9_]*\s*\}\}/;
const IDENTIFIER = String.raw`(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)`;
const OBJECT_NAME_REGEX = new RegExp(
  String.raw`^${IDENTIFIER}(?:\s*\.\s*${IDENTIFIER}){0,2}`,
);

/**
 * Whether `source` is a query, the only kind of statement SUBSCRIBE can wrap.
 * DDL, DML and SET always run once.
 */
export function isQueryStatement(source: string) {
  const statement = stripLeadingTrivia(source);
  return statement.startsWith("(") || QUERY_KEYWORD_REGEX.test(statement);
}

/** Whether a cell runs live, which is whether it is a SUBSCRIBE statement. */
export function runsLive(source: string) {
  return toCellQuery(source).isSubscribe;
}

/** Splits `source` into its leading comments, its statement, and what trails it. */
function splitStatement(source: string) {
  const statement = stripLeadingTrivia(source);
  const leading = source.slice(0, source.length - statement.length);
  const body = trimStatement(statement);
  return { leading, body, trailing: statement.slice(body.length) };
}

/**
 * Wraps a query in `SUBSCRIBE (...)`, keeping its leading comments first and
 * its trailing semicolon or comment last.
 */
export function wrapInSubscribe(source: string) {
  const { leading, body, trailing } = splitStatement(source);
  const wrapped = body.includes("\n")
    ? `SUBSCRIBE (\n${body}\n)`
    : `SUBSCRIBE (${body})`;
  return leading + wrapped + trailing;
}

/**
 * The query a SUBSCRIBE statement subscribes to, with the statement's
 * comments kept. Undefined when it cannot be unwrapped, such as when it has
 * options that a plain query cannot express.
 */
export function unwrapSubscribe(source: string) {
  const cellQuery = toCellQuery(source);
  if (!cellQuery.isSubscribe || cellQuery.error !== undefined) return undefined;
  const { leading, trailing } = splitStatement(source);
  return leading + cellQuery.query.replace(/^\s*\n|\n\s*$/g, "") + trailing;
}

/**
 * Turns a cell's source into the query the worksheet runs.
 *
 * Live cells run as `SUBSCRIBE (<query>) WITH (PROGRESS)`, so a cell written
 * as a `SUBSCRIBE` statement is unwrapped to its query and marked live. Its
 * own options are rejected: `ENVELOPE` and `WITH (...)` change the output
 * columns the live view reduces.
 */
export function toCellQuery(source: string): CellQuery {
  const statement = trimStatement(stripLeadingTrivia(source));
  const prefix = SUBSCRIBE_PREFIX_REGEX.exec(statement);
  if (!prefix) return { query: source, isSubscribe: false };

  const body = statement.slice(prefix[0].length);
  let query: string;
  let rest: string;
  if (body.startsWith("(")) {
    const end = matchParen(body);
    if (end === undefined) {
      return { isSubscribe: true, error: "Unbalanced parentheses" };
    }
    query = body.slice(1, end - 1);
    rest = body.slice(end);
  } else if (QUERY_KEYWORD_REGEX.test(body)) {
    // Not valid Materialize syntax, which requires parentheses, but the
    // intent is unambiguous.
    query = body;
    rest = "";
  } else {
    const relation =
      CELL_REFERENCE_REGEX.exec(body) ?? OBJECT_NAME_REGEX.exec(body);
    if (!relation) {
      return {
        isSubscribe: true,
        error:
          "Expected SUBSCRIBE <object>, SUBSCRIBE (<query>), or SUBSCRIBE {{cell}}",
      };
    }
    query = `SELECT * FROM ${relation[0]}`;
    rest = body.slice(relation[0].length);
  }

  if (stripLeadingTrivia(rest)) {
    return {
      isSubscribe: true,
      error:
        "SUBSCRIBE options are not supported here. Remove them, the cell already runs live.",
    };
  }
  return { query, isSubscribe: true };
}
