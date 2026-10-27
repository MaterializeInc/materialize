// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { lex } from "@materializeinc/sql-lexer";

import { ExtendedRequestItem } from "~/api/materialize/types";
/**
 * Split a query string, potentially containing multiple queries, into a collection of
 * extended queries, with a single query per item.
 *
 * @param queryString - the query to split.
 * @returns a collection of extended query items
 */
export default function splitQueryString(queryString: string) {
  const queries: ExtendedRequestItem[] = [];
  let statementStart = 0;
  try {
    for (const token of lex(queryString)) {
      if (token.kind === "semicolon") {
        queries.push({
          query: queryString.substring(statementStart, token.offset + 1),
        });
        statementStart = token.offset + 1;
      }
    }
  } catch (e) {
    // This may be an irrecoverable WASM error. Normally the `lex` command
    // returns an empty token collection if it is truly unparsable.
    console.error("Could not lex the query");
  }
  if (queries.length === 0) {
    // While the string had contents, the queryString was ultimately unlexable.
    // Send it to the database for a proper error.
    queries.push({ query: queryString });
  }
  return queries;
}
