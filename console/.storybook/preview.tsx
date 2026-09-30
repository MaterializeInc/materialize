// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, ColorModeProvider, useTheme } from "@chakra-ui/react";
import type { Decorator, Preview } from "@storybook/react-vite";
import React from "react";

import { ChakraProviderWrapper } from "~/components/ChakraProviderWrapper";
import { MaterializeTheme, config as themeConfig } from "~/theme";

/**
 * Paints the story's surface with the active theme's page background.
 * Storybook's own canvas is theme-agnostic, so without this a dark-mode story
 * renders light text on white.
 */
const StoryCanvas = ({ children }: React.PropsWithChildren) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Box background={colors.background.primary} padding="6" minHeight="100vh">
      {children}
    </Box>
  );
};

/**
 * Mirrors the provider stack `App.tsx` puts above every screen, minus routing
 * and data fetching. `ColorModeProvider` is given an explicit `value` so the
 * toolbar drives the mode instead of the developer's OS setting.
 */
const withMaterializeTheme: Decorator = (Story, context) => (
  <ColorModeProvider
    options={themeConfig}
    value={context.globals.colorMode as "light" | "dark"}
  >
    <ChakraProviderWrapper>
      <StoryCanvas>
        <Story />
      </StoryCanvas>
    </ChakraProviderWrapper>
  </ColorModeProvider>
);

const preview: Preview = {
  decorators: [withMaterializeTheme],
  initialGlobals: {
    colorMode: "light",
  },
  globalTypes: {
    colorMode: {
      description: "Chakra color mode",
      toolbar: {
        title: "Theme",
        icon: "mirror",
        items: [
          { value: "light", title: "Light" },
          { value: "dark", title: "Dark" },
        ],
        dynamicTitle: true,
      },
    },
  },
  parameters: {
    // The decorator paints the background from the theme instead.
    backgrounds: { disable: true },
    layout: "fullscreen",
    controls: {
      matchers: {
        color: /(background|color)$/i,
        date: /Date$/i,
      },
    },
  },
};

export default preview;
