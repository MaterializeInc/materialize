// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { MenuDivider, MenuGroup, MenuItemOption } from "@chakra-ui/react";
import React from "react";

import { UI_PREVIEWS, UiPreviewKey } from "~/config/uiPreviews";
import { useOfferedUiPreviewKeys, useUiPreview } from "~/hooks/useUiPreview";

const UiPreviewMenuItem = ({ previewKey }: { previewKey: UiPreviewKey }) => {
  const { isEnabled, setOptIn } = useUiPreview(previewKey);
  return (
    <MenuItemOption
      type="checkbox"
      closeOnSelect={false}
      fontWeight="medium"
      isChecked={isEnabled}
      onClick={() => setOptIn(!isEnabled)}
    >
      {UI_PREVIEWS[previewKey].label}
    </MenuItemOption>
  );
};

/** Toggles for the UI previews offered to this user, nothing when none are. */
const UiPreviewSwitcher = () => {
  const offeredKeys = useOfferedUiPreviewKeys();
  if (offeredKeys.length === 0) return null;
  return (
    <>
      <MenuDivider />
      <MenuGroup title="UI previews">
        {offeredKeys.map((key) => (
          <UiPreviewMenuItem key={key} previewKey={key} />
        ))}
      </MenuGroup>
    </>
  );
};

export default UiPreviewSwitcher;
