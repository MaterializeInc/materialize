// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

'use strict';

/**
 * Quote a value as a SQL string literal.
 *
 * What these pages interpolate into a query is attacker-controlled: URL
 * parameters, and catalog names anyone with CREATE on a schema gets to choose.
 * The query then runs in the session of whoever opened the page, so every
 * interpolated value has to go through this or `sqlIdent`. Doubling the quote
 * is the whole of the escaping, as `standard_conforming_strings` is fixed on.
 *
 * @param {string} value - The value to quote
 * @returns {string} - The value as a SQL string literal, quotes included
 */
function sqlLiteral(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

/**
 * Quote a name as a SQL identifier. See `sqlLiteral` on why this is required.
 *
 * @param {string} name - The name to quote
 * @returns {string} - The name as a quoted SQL identifier, quotes included
 */
function sqlIdent(name) {
  return `"${String(name).replace(/"/g, '""')}"`;
}

/**
 * Execute a SQL query against the /api/sql endpoint.
 *
 * @param {string} sql - SQL query string
 * @returns {Promise<Object>} - The parsed JSON response
 */
async function query(sql) {
  const response = await fetch('/api/sql', {
    method: 'POST',
    body: JSON.stringify({ query: sql }),
    headers: { 'Content-Type': 'application/json' },
  });
  if (!response.ok) {
    const text = await response.text();
    throw `request failed: ${response.status} ${response.statusText}: ${text}`;
  }
  const data = await response.json();
  for (const result of data.results) {
    if (result.error) {
      throw `SQL error: ${result.error.message}`;
    }
  }
  return data;
}
