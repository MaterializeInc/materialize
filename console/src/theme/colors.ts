// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

const colors = {
  transparent: "transparent",
  trueBlack: "#000",
  black: "#111",
  /**
   * Light / Background Primary
   * AND
   * Light / Text Inverse
   */
  white: "#FFF",
  offWhite: "#f9f9f9",
  gray: {
    /**
     * Light / Background Secondary
     * AND
     * Dark / Text Inverse
     */
    50: "#F7F7F8",
    /**
     * Light / Background Tertiary
     */
    100: "#ECECEF",
    /**
     * Light / Border Primary
     */
    200: "#EAE9EC",
    /**
     * Light / Border Secondary
     */
    300: "#E0DEE3",
    /**
     * Dark / Forground Secondary
     */
    400: "#BCB9C0",
    /**
     * Light / Text Secondary
     */
    500: "#66626A",
    /**
     * Dark / Border Secondary
     */
    600: "#3D3B40",
    /**
     * Dark / Border Primary
     * AND
     * Dark / Background Tertiary
     */
    700: "#323135",
    /**
     * Dark / Background Secondary
     */
    800: "#232225",
    /**
     * Light / Text Primary
     * AND
     * Dark / Background Primary
     */
    900: "#0D1116",
  },
  purple: {
    50: "#f1ecff",
    100: "#E1D6FF",
    200: "#C8B5FF",
    300: "#B59AFF",
    400: "#7F4EFF", // purple, purple buttons
    500: "#5A34CB", // Primary Materialize Purple
    600: "#472F85", // med-purple
    700: "#391D7E",
    800: "#1B164C", // dark-purple
    900: "#040126", // header bar bg
  },
  // nb: our regular purple is much closer to true "lavender"
  // but that's what the style doc calls it :P
  lavender: {
    50: "#fbe6ff",
    100: "#e6bafa",
    200: "#d28df2",
    300: "#bf60eb",
    400: "#AE37E5", // highlight
    500: "#931acb",
    600: "#7e148f",
    700: "#530d73",
    800: "#320647",
    900: "#1f002c",
  },
  orchid: {
    50: "#ffe5fd",
    100: "#f9b9ed",
    200: "#E37AC2",
    300: "#eb60ce",
    400: "#E537C0", // highlight
    500: "#cb1aa6",
    600: "#9f1381",
    700: "#720b5d",
    800: "#460439",
    900: "#2e0225",
  },
  red: {
    /**
     * Light / Error background
     */
    50: "#ffebeb", // changed from default chakra theme
    /**
     * Light / Error Border
     */
    100: "#ffd1d1", // changed from default chakra theme
    200: "#ff7fa5",
    300: "#ff4d82",
    /**
     * Dark / Accent Red
     */
    400: "#fe1d5e",
    /**
     * Light / Accent Red
     */
    500: "#e50644",
    /**
     * Dark / Error Background
     */
    600: "#803737", // changed from default chakra theme
    /**
     * Dark / Error Border
     */
    700: "#562525", // changed from default chakra theme
    800: "#4f0016",
    900: "#35000f",
  },
  orange: {
    50: "#ffeadf",
    100: "#ffc6b1",
    200: "#FF9067", // used in gradient, closest to highlight color
    300: "#fe7e4e",
    350: "#d9622b",
    400: "#fe581d",
    500: "#e54004",
    600: "#b23101",
    700: "#802200",
    800: "#4e1400",
    900: "#3a0f01",
  },
  yellow: {
    50: "#f9fbd0",
    /**
     * Light / Warning Background
     */
    100: "#fdfee2", // changed from default chakra theme
    200: "#fcfdc9",
    300: "#fafba7",
    /**
     * Light / Warning Border
     */
    400: "#eeef8f", // changed from default chakra theme
    500: "#b7b94d",
    600: "#dde00a",
    700: "#c5c809",
    /**
     * Dark / Warning Border
     */
    800: "#82840a",
    /**
     * Dark / Warning Background
     */
    900: "#354e04",
  },
  honeysuckle: {
    50: "#f6ffdc",
    100: "#eaffaf",
    200: "#deff7f",
    300: "#CBFF38", // highlight
    400: "#bbf320",
    500: "#a2da08",
    600: "#84b300",
    700: "#5e8000",
    800: "#384d00",
    900: "#243100",
  },
  green: {
    50: "#dfffe4",
    100: "#b1ffba",
    200: "#75FF86",
    300: "#4fff63",
    400: "#13D461", // highlight
    450: "#4ca054",
    500: "#07a44a",
    600: "#008a3f",
    700: "#007535",
    800: "#00471d",
    900: "#002e13",
  },
  turquoise: {
    50: "#dbfffe",
    100: "#b5f7f3",
    200: "#8befea",
    300: "#61e8e0",
    400: "#39E1D7", // light button outline
    500: "#1ec7bd",
    600: "#0d9b93",
    700: "#007069",
    800: "#004340",
    900: "#00312f",
  },
  blue: {
    /**
     * Light / Warn Background
     */
    50: "#F0F4FF", // changed from default chakra theme
    /**
     * Light / Warn Border
     */
    100: "#d1d6ff", // changed from default chakra theme
    200: "#7bd1ff",
    300: "#59C3FF",
    400: "#1EAEFF", // highlight
    500: "#0093e6",
    600: "#0072b4",
    /**
     * Dark / Warn Border
     */
    700: "#4e547e", // changed from default chakra theme
    800: "#014166",
    /**
     * Dark / Warn Background
     */
    900: "#2f324c", // changed from default chakra theme
  },
  cobalt: {
    50: "#e3e5ff",
    100: "#b3b8ff",
    200: "#979eff",
    300: "#7d86ff",
    400: "#4f5af5",
    500: "#221EFF", // highlight
    600: "#0000b4",
    700: "#000082",
    800: "#000050",
    900: "#000021",
  },
  indigo: {
    50: "#eee6ff",
    100: "#c8b6ff",
    200: "#a487f9",
    300: "#7f57f5",
    400: "#5C29F1", // for a gradient, too blue for the other purples
    500: "#4512C7",
    600: "#320aa9",
    700: "#25038a",
    800: "#15044b",
    900: "#020025",
  },
};

const gradients = {
  primary: {
    gradient: [
      `radial-gradient(
        88.57% 72.27% at 6.45% 137.95%,
        #9B34CB 0%,
        #9b34cb00 100%
      )`,
      `radial-gradient(
        81.76% 69.63% at 75.55% 107.58%,
        ${colors.purple[500]} 0%,
        #4334cb00 100%
      )`,
    ],
    fallback: colors.indigo[900],
  },
};

const shadows = {
  light: {
    level1: `
      box-shadow: 0px 1px 1px 0px rgba(0, 0, 0, 0.04);
      box-shadow: 0px 1px 3px 0px rgba(0, 0, 0, 0.04);
    `,
    level2: `
      box-shadow: 0px 1px 2px 0px rgba(0, 0, 0, 0.08);
      box-shadow: 0px 2px 2px 0px rgba(0, 0, 0, 0.04);
      box-shadow: 0px 4px 9px 0px rgba(0, 0, 0, 0.04);
    `,
    level3: `
      box-shadow: 0px 1px 2px 0px rgba(0, 0, 0, 0.08);
      box-shadow: 0px 4px 6px 0px rgba(0, 0, 0, 0.06);
      box-shadow: 0px 6px 8px 0px rgba(0, 0, 0, 0.04);
      box-shadow: 0px 8px 16px 0px rgba(0, 0, 0, 0.04);
    `,
    level4: `
      box-shadow: 0px 1px 2px 0px rgba(0, 0, 0, 0.08);
      box-shadow: 0px 12px 20px 0px rgba(0, 0, 0, 0.08);
      box-shadow: 0px 20px 40px 0px rgba(0, 0, 0, 0.08);
    `,
  },
  dark: {
    level1: `
      box-shadow: 0px 1px 2px 0px rgba(0, 0, 0, 0.24);
      box-shadow: 0px 1px 3px 0px rgba(0, 0, 0, 0.16);
    `,
    level2: `
      box-shadow: 0px 1px 2px 0px rgba(0, 0, 0, 0.24);
      box-shadow: 0px 2px 2px 0px rgba(0, 0, 0, 0.24);
      box-shadow: 0px 4px 9px 0px rgba(0, 0, 0, 0.24);
    `,
    level3: `
      box-shadow: 0px 1px 2px 0px rgba(0, 0, 0, 0.24);
      box-shadow: 0px 4px 6px 0px rgba(0, 0, 0, 0.24);
      box-shadow: 0px 6px 8px 0px rgba(0, 0, 0, 0.24);
      box-shadow: 0px 8px 16px 0px rgba(0, 0, 0, 0.24);
    `,
    level4: `
      box-shadow: 0px 1px 2px 0px rgba(0, 0, 0, 0.24);
      box-shadow: 0px 12px 20px 0px rgba(0, 0, 0, 0.24);
      box-shadow: 0px 20px 40px 0px rgba(0, 0, 0, 0.24);
    `,
  },
  footer: "0 -2px 1px #0000000d",
};

export default colors;

export { gradients, shadows };
