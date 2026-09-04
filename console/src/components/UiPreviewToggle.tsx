// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Button, HStack, IconButton, Tag, useTheme } from "@chakra-ui/react";
import React from "react";

import { UiPreviewKey } from "~/config/uiPreviews";
import { useUiPreview } from "~/hooks/useUiPreview";
import { CloseIcon, ReturnArrowIcon, SparklesIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";

export interface UiPreviewToggleProps {
  previewKey: UiPreviewKey;
  /** Call to action shown until the user opts in, e.g. "Try the new shell experience". */
  label: string;
  /** Shown while opted in, offering the way back. */
  optOutLabel?: string;
}

/**
 * Opt-in pill for a UI preview, for the page the preview changes. While opted
 * in it offers the way back, tagged "Preview". Crossing it away collapses it
 * to a sparkle for the rest of the tab session; the sparkle expands it again.
 * Renders nothing when the preview isn't offered to this user.
 */
export const UiPreviewToggle = ({
  previewKey,
  label,
  optOutLabel = "Switch to classic UI",
}: UiPreviewToggleProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { isAvailable, isEnabled, setOptIn, isCollapsed, setCollapsed } =
    useUiPreview(previewKey);
  const sparkleRef = React.useRef<HTMLButtonElement>(null);
  const pillRef = React.useRef<HTMLButtonElement>(null);
  const interactedRef = React.useRef(false);
  // Collapsing unmounts the focused button, so hand focus to its counterpart.
  // The interacted guard keeps mount (and a restored collapse) from stealing focus.
  React.useEffect(() => {
    if (!interactedRef.current) return;
    (isCollapsed ? sparkleRef : pillRef).current?.focus();
  }, [isCollapsed]);
  const toggleCollapsed = (collapsed: boolean) => {
    interactedRef.current = true;
    setCollapsed(collapsed);
  };
  if (!isAvailable) return null;
  if (isCollapsed) {
    return (
      <IconButton
        ref={sparkleRef}
        aria-label="Expand preview"
        icon={<SparklesIcon boxSize={4} color="currentColor" />}
        size="sm"
        background={colors.background.accent}
        color={colors.accent.brightPurple}
        borderRadius="lg"
        _hover={{ opacity: 0.8 }}
        _active={{ opacity: 0.7 }}
        onClick={() => toggleCollapsed(false)}
      />
    );
  }
  return (
    <HStack spacing={0} background={colors.background.accent} borderRadius="lg">
      <Button
        ref={pillRef}
        size="sm"
        leftIcon={
          isEnabled ? (
            <ReturnArrowIcon boxSize={4} color="currentColor" />
          ) : (
            <SparklesIcon boxSize={4} color="currentColor" />
          )
        }
        rightIcon={
          isEnabled ? (
            <Tag size="sm" variant="outline" colorScheme="lavender">
              Preview
            </Tag>
          ) : undefined
        }
        background="transparent"
        color={colors.accent.brightPurple}
        _hover={{ opacity: 0.8 }}
        _active={{ opacity: 0.7 }}
        onClick={() => setOptIn(!isEnabled)}
      >
        {isEnabled ? optOutLabel : label}
      </Button>
      <IconButton
        aria-label="Collapse preview"
        icon={
          // CloseIcon's paths carry no stroke attribute, so pass one explicitly.
          <CloseIcon boxSize={3} color="currentColor" stroke="currentColor" />
        }
        size="sm"
        background="transparent"
        color={colors.accent.brightPurple}
        _hover={{ opacity: 0.8 }}
        _active={{ opacity: 0.7 }}
        onClick={() => toggleCollapsed(true)}
      />
    </HStack>
  );
};
