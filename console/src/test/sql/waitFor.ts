// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Awaits the provided function until it complete successfully or fails after the
 * ellapsed timeout.
 */
export async function waitFor(fn: () => Promise<unknown>, timeout = 3000) {
  const start = performance.now();
  let finished = false;
  while (!finished) {
    try {
      await fn();
      return;
    } catch (e) {
      const elapsed = performance.now() - start;
      if (elapsed >= timeout) {
        finished = true;
        console.error(`waitFor timed out after ${timeout}ms`);
        throw e;
      }
    }
  }
}
