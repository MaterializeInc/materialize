// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useAtom } from "jotai";
import { atomFamily, atomWithStorage, createJSONStorage } from "jotai/utils";

import { UI_PREVIEWS, UiPreviewKey } from "~/config/uiPreviews";
import { useAppConfig } from "~/config/useAppConfig";
import { useFlags } from "~/hooks/useFlags";

export const uiPreviewOptInStorageKey = (key: UiPreviewKey) =>
  `uiPreviewOptIn:${key}`;

export const uiPreviewCollapsedStorageKey = (key: UiPreviewKey) =>
  `uiPreviewCollapsed:${key}`;

// Per-browser opt-ins. The LaunchDarkly flag only controls whether a preview
// is offered, so it can be pulled for everyone at once.
const optInAtomFamily = atomFamily((key: UiPreviewKey) =>
  atomWithStorage(uiPreviewOptInStorageKey(key), false, undefined, {
    getOnInit: true,
  }),
);

// Collapse lasts for the tab session only, the full pill returns next visit.
const collapsedAtomFamily = atomFamily((key: UiPreviewKey) =>
  atomWithStorage(
    uiPreviewCollapsedStorageKey(key),
    false,
    createJSONStorage(() => sessionStorage),
    { getOnInit: true },
  ),
);

/** Gates a UI preview: offered via its LaunchDarkly flag, enabled per user. */
export const useUiPreview = (key: UiPreviewKey) => {
  const appConfig = useAppConfig();
  const flags = useFlags();
  const [optIn, setOptIn] = useAtom(optInAtomFamily(key));
  const [isCollapsed, setCollapsed] = useAtom(collapsedAtomFamily(key));
  // Previews are a cloud experiment mechanism. Self-managed builds have no
  // LaunchDarkly, so they ship the gated UI outright and offer no choice.
  const isCloud = appConfig.mode === "cloud";
  const flagOn = Boolean(flags[UI_PREVIEWS[key].ldFlag]);
  return {
    /** Whether to offer the preview toggle to this user at all. */
    isAvailable: isCloud && flagOn,
    /** Whether to render the new UI. */
    isEnabled: isCloud ? flagOn && optIn : flagOn,
    setOptIn,
    /** Whether the on-page pill is collapsed to its icon. The profile menu ignores this. */
    isCollapsed,
    setCollapsed,
  };
};

/** Keys of the previews offered to this user, for listing their toggles. */
export const useOfferedUiPreviewKeys = (): UiPreviewKey[] => {
  const appConfig = useAppConfig();
  const flags = useFlags();
  if (appConfig.mode !== "cloud") return [];
  return (Object.keys(UI_PREVIEWS) as UiPreviewKey[]).filter((key) =>
    Boolean(flags[UI_PREVIEWS[key].ldFlag]),
  );
};
