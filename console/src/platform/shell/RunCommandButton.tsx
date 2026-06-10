// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Button, ButtonProps, Tooltip, useTheme } from "@chakra-ui/react";
import { useAtom } from "jotai";
import { useAtomCallback } from "jotai/utils";
import React from "react";

import PlayIcon from "~/svg/PlayIcon";
import StopIcon from "~/svg/StopIcon";
import { MaterializeTheme } from "~/theme";
import { controlOrCommand } from "~/util";

import { isCommandProcessing as webSocketCommandProcessing } from "./machines/webSocketFsm";
import { currentPromptValue, saveClearPrompt } from "./store/prompt";
import { shellStateAtom } from "./store/shell";
import useShellWebsocket from "./useShellWebsocket";

type RunCommandButtonProps = ButtonProps & {
  runCommand: (command: string) => void;

  cancelQuery: () => void;
};

const RunCommandButton = ({
  runCommand,
  cancelQuery,
  ...rest
}: RunCommandButtonProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { isSocketAvailable, isSocketError, isSocketInitializing } =
    useShellWebsocket();
  const [prompt] = useAtom(currentPromptValue);
  const clearPrompt = useAtomCallback(saveClearPrompt);
  const [{ webSocketState }] = useAtom(shellStateAtom);

  const isStreaming = webSocketState === "commandInProgressStreaming";

  const isCommandProcessing =
    webSocketState && webSocketCommandProcessing(webSocketState);

  const isButtonDisabled =
    !isSocketAvailable || (!isCommandProcessing && prompt.trim().length === 0);

  const isConnectionLost = !isSocketInitializing && isSocketError;

  let buttonText = "Run Query";
  if (isConnectionLost) {
    buttonText = "Reconnect";
  } else if (isStreaming) {
    buttonText = "Stop Streaming";
  } else if (isCommandProcessing) {
    buttonText = "Cancel Query";
  }

  const leftIcon = isConnectionLost ? undefined : isCommandProcessing ? (
    <StopIcon />
  ) : (
    <PlayIcon />
  );

  // TODO: Support the keyboard shortcut for canceling queries too.
  const kbdModifier = controlOrCommand();

  const tooltipText = isCommandProcessing
    ? "Cancel your query"
    : `Run your query (${kbdModifier} + Enter)`;

  return (
    <Tooltip
      label={tooltipText}
      isDisabled={isButtonDisabled}
      background={colors.background.secondary}
      color={colors.foreground.secondary}
    >
      <Button
        variant={isCommandProcessing ? "primary" : "tertiary"}
        position="absolute"
        right="3"
        bottom="3"
        leftIcon={leftIcon}
        onClick={() => {
          if (isCommandProcessing) {
            cancelQuery();
          } else if (prompt.trim().length > 0) {
            runCommand(prompt);
            clearPrompt();
          }
        }}
        isDisabled={isButtonDisabled}
        loadingText={buttonText}
        {...rest}
        _hover={{
          opacity: isButtonDisabled ? 0.4 : 1,
        }}
      >
        {buttonText}
      </Button>
    </Tooltip>
  );
};

export default RunCommandButton;
