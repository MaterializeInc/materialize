// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Returns a single abort signal that aborts if any provided signal is aborted.
 *
 * `AbortSignal.any` is actually supported in all major browsers now, but only very
 * recently: https://developer.mozilla.org/en-US/docs/Web/API/AbortSignal/any_static.
 * It's also not typescript just yet, so I figured we can use this simple implementation
 * for now.
 *
 * Source: https://github.com/whatwg/fetch/issues/905#issuecomment-1816547024
 */
export function anySignal(signals: AbortSignal[]): AbortSignal {
  const controller = new AbortController();

  for (const signal of signals) {
    if (signal.aborted) {
      controller.abort();
      return signal;
    }

    signal.addEventListener("abort", () => controller.abort(signal.reason), {
      signal: controller.signal,
    });
  }

  return controller.signal;
}
