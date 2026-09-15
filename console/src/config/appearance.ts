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
 * Per-instance appearance, set on the Materialize resource and delivered to
 * the browser in app-config.json. Self-managed only, since Cloud serves one
 * console for all of a user's regions.
 */

export interface ConsoleAppearance {
  /** A short name for the instance, such as "dev" or "prod". */
  displayName?: string;
}

const BASE_DOCUMENT_TITLE = "Materialize Console";

/** Builds the browser tab title, which names the instance when configured. */
export const documentTitle = (appearance: ConsoleAppearance | undefined) =>
  appearance?.displayName
    ? `${BASE_DOCUMENT_TITLE} · ${appearance.displayName}`
    : BASE_DOCUMENT_TITLE;

/** Reads the appearance out of app-config.json. */
export const parseConsoleAppearance = (
  appearance: { displayName?: string } | null | undefined,
): ConsoleAppearance | undefined => {
  if (!appearance) {
    return undefined;
  }
  return { displayName: appearance.displayName || undefined };
};
