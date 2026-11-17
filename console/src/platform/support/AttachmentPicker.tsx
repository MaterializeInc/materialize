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

import {
  ATTACHMENT_ACCEPT,
  ATTACHMENT_LIMITS,
  attachmentFileName,
  MAX_ATTACHMENTS_BYTES,
} from "./ticketForm";

/** Files with the same metadata may be the same file picked again. */
const fileId = (file: File) => `${file.name}:${file.size}:${file.lastModified}`;

const readBytes = (file: Blob) =>
  new Promise<Uint8Array>((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(new Uint8Array(reader.result as ArrayBuffer));
    reader.onerror = () => reject(reader.error);
    reader.readAsArrayBuffer(file);
  });

/**
 * Whether two files with the same metadata also have the same contents.
 * Same-named logs from different folders can share a size and timestamp.
 */
const isSameFile = async (a: File, b: File) => {
  if (a === b) return true;
  // Too large to attach anyway, so not worth reading: keep both and let the
  // form say why.
  if (a.size > MAX_ATTACHMENTS_BYTES) return false;
  try {
    const [left, right] = await Promise.all([readBytes(a), readBytes(b)]);
    return left.every((byte, i) => byte === right[i]);
  } catch {
    // Keep both rather than drop a file that may be different.
    return false;
  }
};

/** The files of `picked` that aren't already in `files`. */
const newFiles = async (files: File[], picked: File[]) => {
  const added: File[] = [];
  for (const file of picked) {
    const sameMetadata = [...files, ...added].filter(
      (other) => fileId(other) === fileId(file),
    );
    const repeats = await Promise.all(
      sameMetadata.map((other) => isSameFile(other, file)),
    );
    if (!repeats.some(Boolean)) added.push(file);
  }
  return added;
};

/** Whether any picked file shares metadata with a file before it. */
const mayRepeat = (files: File[], picked: File[]) => {
  const ids = new Set(files.map(fileId));
  return picked.some((file) => {
    const id = fileId(file);
    if (ids.has(id)) return true;
    ids.add(id);
    return false;
  });
};

const sameFiles = (a: File[], b: File[]) =>
  a.length === b.length && a.every((file, i) => file === b[i]);

/**
 * Files to add to the ticket once it's open. The button's tooltip gives the
 * limits; the global API checks them again.
 */
export const AttachmentPicker = ({
  files,
  error,
  isDisabled,
  onChange,
  onCheckingChange,
}: {
  files: File[];
  error: string | undefined;
  /** While the ticket opens: the files it will get are already set. */
  isDisabled: boolean;
  onChange: (files: File[]) => void;
  /**
   * Whether picked files are still being compared with ones already picked.
   * They aren't in `files` until that's done, so the ticket should wait.
   */
  onCheckingChange: (checking: boolean) => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const input = React.useRef<HTMLInputElement>(null);
  const attachButton = React.useRef<HTMLButtonElement>(null);
  const list = React.useRef<HTMLUListElement>(null);
  // Comparing contents takes a moment, and files can be removed meanwhile.
  const latestFiles = React.useRef(files);
  React.useEffect(() => {
    latestFiles.current = files;
  });
  // Picks wait for any check before them, so they're added in order and each
  // is compared with every file picked earlier.
  const checks = React.useRef(Promise.resolve());
  const pendingChecks = React.useRef(0);

  const setFiles = (next: File[]) => {
    latestFiles.current = next;
    onChange(next);
  };

  const addFiles = (picked: File[]) => {
    if (
      pendingChecks.current === 0 &&
      !mayRepeat(latestFiles.current, picked)
    ) {
      setFiles([...latestFiles.current, ...picked]);
      return;
    }
    // Only files that share metadata need their contents compared.
    pendingChecks.current += 1;
    onCheckingChange(true);
    checks.current = checks.current.then(async () => {
      let compared: File[];
      let added: File[];
      // Files can be removed while the contents are read: if so, compare
      // again with the files that are left.
      do {
        compared = latestFiles.current;
        added = await newFiles(compared, picked);
      } while (!sameFiles(compared, latestFiles.current));
      if (added.length > 0) {
        setFiles([...latestFiles.current, ...added]);
      }
      pendingChecks.current -= 1;
      // In the same update as the files, so the ticket never sees one
      // without the other.
      if (pendingChecks.current === 0) onCheckingChange(false);
    });
  };

  const removeFile = (index: number) => {
    setFiles(latestFiles.current.filter((_, i) => i !== index));
    // Keep keyboard focus in the picker after the removed control unmounts:
    // on the next file's remove button, else the last one, else Attach files.
    queueMicrotask(() => {
      const buttons =
        list.current?.querySelectorAll<HTMLButtonElement>("[data-remove-file]");
      const next = buttons?.[Math.min(index, buttons.length - 1)];
      (next ?? attachButton.current)?.focus();
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
          const picked = Array.from(event.target.files ?? []);
          // Lets the same file be picked again after it's removed.
          event.target.value = "";
          addFiles(picked);
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
        <VStack
          ref={list}
          as="ul"
          listStyleType="none"
          spacing="1"
          align="stretch"
          mt="2"
        >
          {files.map((file, index) => (
            <HStack
              as="li"
              // Different files can share metadata, so count earlier ones.
              key={`${fileId(file)}#${
                files
                  .slice(0, index)
                  .filter((earlier) => fileId(earlier) === fileId(file)).length
              }`}
              spacing="2"
            >
              {/* The name it's sent with: hidden characters are left out. */}
              <Text textStyle="text-small" flex="1" minWidth="0" noOfLines={1}>
                {attachmentFileName(file.name)}
              </Text>
              <Text textStyle="text-small" color={colors.foreground.secondary}>
                {formatBytesShort(BigInt(file.size))}
              </Text>
              <IconButton
                data-remove-file
                aria-label={`Remove ${attachmentFileName(file.name)}`}
                icon={<CloseIcon boxSize={3} />}
                variant="ghost"
                size="xs"
                isDisabled={isDisabled}
                onClick={() => removeFile(index)}
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
