// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import type { StorybookConfig } from "@storybook/react-vite";
import type { PluginOption } from "vite";

/**
 * Plugins from the app's `vite.config.ts` that must not reach Storybook.
 *
 * Storybook renders stories in an iframe whose HTML it generates itself.
 * `vite-plugin-html` is configured with the app's own entry point, so leaving
 * it in boots the entire console inside that iframe alongside the story. The
 * Sentry plugin only uploads source maps and needs credentials we don't have
 * here.
 *
 * Everything else is inherited, so `~/` path aliases, SVG imports, WASM, and
 * the `__BASENAME__`-style build constants keep working with no second copy of
 * that configuration to maintain.
 */
const EXCLUDED_VITE_PLUGINS = ["html", "sentry"];

function flattenPlugins(plugins: PluginOption[]): PluginOption[] {
  return plugins.flatMap((plugin) =>
    Array.isArray(plugin) ? flattenPlugins(plugin) : [plugin],
  );
}

const config: StorybookConfig = {
  stories: ["../src/**/*.stories.@(ts|tsx)"],
  framework: {
    name: "@storybook/react-vite",
    options: {},
  },
  /**
   * Storybook composes a remote Storybook for any dependency that publishes
   * one, which here means both the build and the manager fetch Chakra's
   * published Storybook and list hundreds of its stories beside ours. It
   * tracks Chakra's current major rather than the version this app pins, so
   * it advertises components we do not have, and it makes a third-party
   * request from a developer tool that then fails offline.
   *
   * A function replaces the detected set, where an object would be merged into
   * it, so this stays correct if another dependency starts publishing one.
   */
  refs: () => ({}),
  viteFinal: async (viteConfig, { configType }) => {
    viteConfig.plugins = flattenPlugins(viteConfig.plugins ?? []).filter(
      (plugin) => {
        const name =
          plugin && typeof plugin === "object" && "name" in plugin
            ? String(plugin.name)
            : "";
        return !EXCLUDED_VITE_PLUGINS.some((excluded) =>
          name.includes(excluded),
        );
      },
    );

    // Storybook deploys on its own, so it has no use for the app's `public/`
    // directory, which holds the console's runtime config, favicon and logo.
    // Storybook emits the assets its own UI needs, and the app's fonts are
    // imported from `src/` rather than served from `public/`.
    if (configType === "PRODUCTION") {
      viteConfig.publicDir = false;
    }

    return viteConfig;
  },
};

export default config;
