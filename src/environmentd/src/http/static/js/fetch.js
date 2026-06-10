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
