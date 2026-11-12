// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CloseIcon } from "@chakra-ui/icons";
import {
  Box,
  Button,
  HStack,
  IconButton,
  Text,
  Tooltip,
  useTheme,
  VisuallyHiddenInput,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import { PaperclipIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";
import { formatBytesShort } from "~/utils/format";

import { ATTACHMENT_ACCEPT, ATTACHMENT_LIMITS } from "./ticketForm";

/** A picked file, with an ID to key it by: files have none of their own. */
export type PickedFile = { id: number; file: File };

/**
 * Files to add to the ticket once it's open. The button's tooltip gives the
 * limits; the global API checks them again.
 */
export const AttachmentPicker = ({
  files,
  error,
  isDisabled,
  onChange,
}: {
  files: PickedFile[];
  error: string | undefined;
  /** While the ticket opens: the files it will get are already set. */
  isDisabled: boolean;
  onChange: (files: PickedFile[]) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const input = React.useRef<HTMLInputElement>(null);
  const attachButton = React.useRef<HTMLButtonElement>(null);
  const removeButtons = React.useRef(new Map<number, HTMLButtonElement>());
  const nextId = React.useRef(0);

  const removeFile = (id: number) => {
    const index = files.findIndex((picked) => picked.id === id);
    const remaining = files.filter((picked) => picked.id !== id);
    onChange(remaining);
    // Keep keyboard focus in the picker after the removed control unmounts.
    queueMicrotask(() => {
      const next = remaining[index] ?? remaining[index - 1];
      if (next) {
        removeButtons.current.get(next.id)?.focus();
      } else {
        attachButton.current?.focus();
      }
    });
  };

  return (
    <Box data-testid="support-ticket-attachments">
      <VisuallyHiddenInput
        ref={input}
        type="file"
        multiple
        accept={ATTACHMENT_ACCEPT}
        tabIndex={-1}
        disabled={isDisabled}
        aria-label="Files to attach"
        data-testid="support-ticket-file-input"
        onChange={(event) => {
          const added = Array.from(event.target.files ?? [], (file) => ({
            id: nextId.current++,
            file,
          }));
          onChange([...files, ...added]);
          // Lets the same file be picked again after it's removed.
          event.target.value = "";
        }}
      />
      <Tooltip label={ATTACHMENT_LIMITS} lineHeight={1.2} hasArrow>
        <Button
          ref={attachButton}
          variant="secondary"
          size="sm"
          leftIcon={<PaperclipIcon />}
          isDisabled={isDisabled}
          onClick={() => input.current?.click()}
        >
          Attach files
        </Button>
      </Tooltip>
      {files.length > 0 && (
        <VStack as="ul" listStyleType="none" spacing="1" align="stretch" mt="2">
          {files.map(({ id, file }) => (
            <HStack as="li" key={id} spacing="2">
              <Text textStyle="text-small" flex="1" minWidth="0" noOfLines={1}>
                {file.name}
              </Text>
              <Text textStyle="text-small" color={colors.foreground.secondary}>
                {formatBytesShort(BigInt(file.size))}
              </Text>
              <IconButton
                ref={(node) => {
                  if (node) {
                    removeButtons.current.set(id, node);
                  } else {
                    removeButtons.current.delete(id);
                  }
                }}
                aria-label={`Remove ${file.name}`}
                icon={<CloseIcon boxSize={3} />}
                variant="ghost"
                size="xs"
                isDisabled={isDisabled}
                onClick={() => removeFile(id)}
              />
            </HStack>
          ))}
        </VStack>
      )}
      {error && (
        <Text
          role="alert"
          textStyle="text-small"
          color={colors.accent.red}
          mt="2"
        >
          {error}
        </Text>
      )}
      <Text textStyle="text-small" color={colors.foreground.secondary} mt="2">
        Please make sure files don&apos;t include passwords, credentials, or
        other sensitive data.
      </Text>
    </Box>
  );
};
