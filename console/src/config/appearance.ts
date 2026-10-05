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

/**
 * The hues an instance may accent its console with. Each names a hue of the
 * palette, which the theme reads the appropriate shades of. See
 * `~/theme/accent`.
 */
export const ACCENT_COLORS = ["blue", "orange", "purple"] as const;

export type AccentColor = (typeof ACCENT_COLORS)[number];

export interface ConsoleAppearance {
  /** A short name for the instance, such as "dev" or "prod". */
  displayName?: string;
  accentColor?: AccentColor;
}

const BASE_DOCUMENT_TITLE = "Materialize Console";

/** Builds the browser tab title, which names the instance when configured. */
export const documentTitle = (appearance: ConsoleAppearance | undefined) =>
  appearance?.displayName
    ? `${BASE_DOCUMENT_TITLE} · ${appearance.displayName}`
    : BASE_DOCUMENT_TITLE;

const isAccentColor = (value: unknown): value is AccentColor =>
  ACCENT_COLORS.includes(value as AccentColor);

/**
 * Validates the appearance read out of app-config.json.
 *
 * Orchestratord writes that file and may be newer than the console it serves,
 * so an accent color this build doesn't know is dropped rather than handed to
 * the theme as an unresolvable color.
 */
export const parseConsoleAppearance = (
  appearance: { displayName?: string; accentColor?: string } | null | undefined,
): ConsoleAppearance | undefined => {
  if (!appearance) {
    return undefined;
  }
  return {
    displayName: appearance.displayName || undefined,
    accentColor: isAccentColor(appearance.accentColor)
      ? appearance.accentColor
      : undefined,
  };
};
