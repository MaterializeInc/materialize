// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Text, TextProps, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";

import {
  attachFilesMessage,
  isAttachUnconfirmedError,
  SHARE_FILES_INSTEAD,
  useAttachFilesState,
} from "./queries";

/**
 * How adding files to a new ticket is going. Shows nothing for tickets
 * opened without files, or once the session has forgotten them.
 */
export const AttachFilesStatus = ({
  ticketId,
  showShareInstead = false,
  ...props
}: TextProps & {
  ticketId: string;
  /** Says how to share files that couldn't be added. */
  showShareInstead?: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const attach = useAttachFilesState(ticketId);
  const message = attach && attachFilesMessage(attach);
  if (!attach || !message) return null;
  const isError = attach.status === "error";

  return (
    <Text
      textStyle="text-small"
      color={isError ? colors.accent.red : colors.foreground.secondary}
      // Keep a stable role: swapping status/alert mid-update is announced
      // unreliably by screen readers.
      role="status"
      aria-live={isError ? "assertive" : "polite"}
      data-testid="support-ticket-attachments-status"
      {...props}
    >
      {showShareInstead && isError && !isAttachUnconfirmedError(attach.error)
        ? `${message} ${SHARE_FILES_INSTEAD}`
        : message}
    </Text>
  );
};
