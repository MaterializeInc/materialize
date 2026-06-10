// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalHeader,
  ModalOverlay,
  Text,
  useTheme,
} from "@chakra-ui/react";
import React from "react";

import McpConnectInstructions from "~/components/McpConnectInstructions";
import { Modal } from "~/components/Modal";
import ConnectionIcon from "~/svg/ConnectionIcon";
import { MaterializeTheme } from "~/theme";

import { TabbedCodeBlock } from "./copyableComponents";

const PASSWORD_USERNAME_PLACEHOLDER = "<your_username>";

const PasswordConnectModal = ({
  onClose,
  isOpen,
}: {
  onClose: () => void;
  isOpen: boolean;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Modal size="3xl" isOpen={isOpen} onClose={onClose}>
      <ModalOverlay />
      <ModalContent>
        <ModalHeader fontWeight="500">Connect To Materialize</ModalHeader>
        <ModalCloseButton />
        <ModalBody pt="2" pb="6" alignItems="stretch">
          <Text
            fontSize="sm"
            whiteSpace="normal"
            color={colors.foreground.secondary}
          >
            Use the details below to connect your AI agent to Materialize.
          </Text>
          <TabbedCodeBlock
            mt="4"
            tabs={[
              {
                title: "MCP Server",
                children: (
                  <McpConnectInstructions
                    userStr={PASSWORD_USERNAME_PLACEHOLDER}
                  />
                ),
                icon: <ConnectionIcon w="4" h="4" />,
              },
            ]}
            minHeight="208px"
          />
        </ModalBody>
      </ModalContent>
    </Modal>
  );
};

export default PasswordConnectModal;
