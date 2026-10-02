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
 * The hue the console accents itself with: the active navigation item, primary
 * buttons, links, focus rings, selection highlights, and the like.
 *
 * Self-managed instances can point this at another hue so that their consoles
 * are distinguishable. Colors that carry meaning are left alone, so an error
 * stays red and a healthy object stays green whatever the accent.
 */

import { appConfig } from "~/config/AppConfig";
import { type AccentColor } from "~/config/appearance";

import colors from "./colors";

/** Materialize purple, used when an instance configures nothing. */
export const DEFAULT_ACCENT_COLOR: AccentColor = "purple";

/**
 * One hue of the palette, from its palest shade to its deepest.
 *
 * Every hue carries at least these shades, so a component can read the same
 * position whichever hue is configured.
 */
export type AccentHue = typeof colors.purple;

type Shade = keyof AccentHue;

/**
 * The shades each hue accents with.
 *
 * The palette's hues do not run parallel: a shade that is vivid in one hue is
 * muted or washed out in another, so each hue names its own. `primary` is a
 * surface behind white label text and `bright` is drawn as text on the page,
 * so both are kept at 4.5:1 or better against what they sit against. Purple
 * is the default, and names the shades the rest of the palette was designed
 * around.
 *
 * TODO(#38691): a palette drawn as an even scale, rather than shades picked
 * out of hues that were never meant to be interchangeable, would let this be
 * a single rule instead of a table. That needs design input.
 */
const ACCENT_SHADES: Record<
  AccentColor,
  { light: { primary: Shade; bright: Shade }; dark: { primary: Shade } }
> = {
  purple: { light: { primary: 600, bright: 500 }, dark: { primary: 400 } },
  orange: { light: { primary: 600, bright: 600 }, dark: { primary: 500 } },
  blue: { light: { primary: 600, bright: 600 }, dark: { primary: 600 } },
};

/**
 * The shade the dark theme draws accented text in.
 *
 * The dark themes accent against one background rather than the palette's
 * whole range, so a single pale shade reads for every hue.
 */
const DARK_BRIGHT_SHADE: Shade = 300;

/** The shade the light theme rings a focused input with. */
const LIGHT_FOCUS_SHADE: Shade = 400;

// Opacity of the wash behind hovered navigation items.
const WASH_ALPHA = 0.08;
// Opacity of the wash behind the active navigation item.
const ACTIVE_WASH_ALPHA = 0.2;
// Opacity of the ring around a focused input.
const LIGHT_FOCUS_RING_ALPHA = 0.24;
const DARK_FOCUS_RING_ALPHA = 0.4;

/** `hex` at `alpha` opacity. */
const withAlpha = (hex: string, alpha: number) => {
  const [r, g, b] = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16));
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
};

/** The hue an accent color names. */
export const accentHueFor = (accentColor: AccentColor | undefined): AccentHue =>
  colors[accentColor ?? DEFAULT_ACCENT_COLOR];

export interface AccentPalette {
  /** Accented surfaces, such as a primary button's background. */
  primary: string;
  /** Accented detail against a page background: links, borders, icons. */
  bright: string;
  /** A wash of the accent, for accented backgrounds behind ordinary text. */
  wash: string;
  /** The ring drawn around a focused input. */
  focusRing: string;
}

export interface AccentPalettes {
  light: AccentPalette;
  dark: AccentPalette;
  /**
   * The wash behind the active navigation item, stronger than `wash` and
   * shared by both themes.
   */
  activeWash: string;
}

/** Both themes' accent tokens for an accent color. */
export const buildAccentPalettes = (
  accentColor: AccentColor | undefined,
): AccentPalettes => {
  const hue = accentHueFor(accentColor);
  const shades = ACCENT_SHADES[accentColor ?? DEFAULT_ACCENT_COLOR];
  const darkBright = hue[DARK_BRIGHT_SHADE];
  const lightBright = hue[shades.light.bright];

  return {
    light: {
      primary: hue[shades.light.primary],
      bright: lightBright,
      wash: withAlpha(lightBright, WASH_ALPHA),
      focusRing: `0px 0px 0px 2px ${withAlpha(
        hue[LIGHT_FOCUS_SHADE],
        LIGHT_FOCUS_RING_ALPHA,
      )}`,
    },
    dark: {
      primary: hue[shades.dark.primary],
      bright: darkBright,
      wash: withAlpha(darkBright, WASH_ALPHA),
      focusRing: `0px 0px 0px 2px ${withAlpha(
        darkBright,
        DARK_FOCUS_RING_ALPHA,
      )}`,
    },
    activeWash: withAlpha(lightBright, ACTIVE_WASH_ALPHA),
  };
};

export const accentHue = accentHueFor(appConfig.appearance?.accentColor);

export const accentPalettes = buildAccentPalettes(
  appConfig.appearance?.accentColor,
);
