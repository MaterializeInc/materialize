// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ACCENT_COLORS } from "~/config/appearance";

import { accentHueFor, buildAccentPalettes } from "./accent";
import colors from "./colors";
import { darkColors } from "./dark";
import { lightColors } from "./light";

describe("accentHueFor", () => {
  it("defaults to Materialize purple", () => {
    expect(accentHueFor(undefined)).toBe(colors.purple);
  });

  it("resolves a configured accent color to its hue", () => {
    expect(accentHueFor("blue")).toBe(colors.blue);
  });
});

describe("buildAccentPalettes", () => {
  // An instance that configures nothing must render exactly as it did before
  // the accent became configurable, so these pin the shades the themes used
  // when they were hardcoded.
  it("reproduces the original purple tokens by default", () => {
    const { light, dark } = buildAccentPalettes(undefined);

    expect(light).toEqual({
      primary: "#472F85",
      bright: "#5A34CB",
      wash: "rgba(90, 52, 203, 0.08)",
      focusRing: "0px 0px 0px 2px rgba(127, 78, 255, 0.24)",
    });
    expect(dark).toEqual({
      primary: "#7F4EFF",
      bright: "#B59AFF",
      wash: "rgba(181, 154, 255, 0.08)",
      focusRing: "0px 0px 0px 2px rgba(181, 154, 255, 0.4)",
    });
  });

  it("washes the active navigation item the same in both themes", () => {
    expect(buildAccentPalettes(undefined).activeWash).toEqual(
      "rgba(90, 52, 203, 0.2)",
    );
  });

  it("accents a configured hue with that hue's shades", () => {
    const { light, dark } = buildAccentPalettes("orange");

    expect(light.primary).toEqual(colors.orange[600]);
    expect(light.bright).toEqual(colors.orange[600]);
    expect(dark.primary).toEqual(colors.orange[500]);
    expect(dark.bright).toEqual(colors.orange[300]);
  });

  it("washes the accent at the same opacity for any hue", () => {
    const { light, dark } = buildAccentPalettes("blue");

    expect(light.wash).toEqual("rgba(0, 114, 180, 0.08)");
    expect(dark.wash).toEqual("rgba(89, 195, 255, 0.08)");
  });
});

/** WCAG relative luminance of a `#rgb` or `#rrggbb` color. */
const luminance = (hex: string) => {
  const digits = hex.replace("#", "");
  const width = digits.length / 3;
  const channels = [0, 1, 2]
    .map((i) => digits.slice(i * width, (i + 1) * width))
    .map((channel) => (width === 1 ? channel.repeat(2) : channel))
    .map((channel) => parseInt(channel, 16) / 255)
    .map((c) => (c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4));
  return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2];
};

const contrast = (a: string, b: string) => {
  const [dimmer, brighter] = [luminance(a), luminance(b)].sort((x, y) => x - y);
  return (brighter + 0.05) / (dimmer + 0.05);
};

describe("accent shades", () => {
  // The shades are chosen by hand per hue, so hold them to the ratios that
  // choice promises rather than trusting the table.
  it.each(ACCENT_COLORS)("stay legible for %s", (accentColor) => {
    const { light, dark } = buildAccentPalettes(accentColor);

    // Drawn as text on the page, and as a surface behind a white label.
    expect(
      contrast(light.bright, lightColors.background.primary),
    ).toBeGreaterThanOrEqual(4.5);
    expect(
      contrast(light.primary, lightColors.foreground.primaryButtonLabel),
    ).toBeGreaterThanOrEqual(4.5);
    expect(
      contrast(dark.bright, darkColors.background.secondary),
    ).toBeGreaterThanOrEqual(4.5);
    // Large, bold button labels, which WCAG holds to 3:1.
    expect(
      contrast(dark.primary, darkColors.foreground.primaryButtonLabel),
    ).toBeGreaterThanOrEqual(3);
  });
});
