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
} from "@chakra-ui/react";
import React from "react";

import McpConnectInstructions from "~/components/McpConnectInstructions";
import { Modal } from "~/components/Modal";

export interface McpConnectModalProps {
  onClose: () => void;
  isOpen: boolean;
  userStr?: string;
}

const McpConnectModal = ({
  onClose,
  isOpen,
  userStr,
}: McpConnectModalProps) => {
  return (
    <Modal size="3xl" isOpen={isOpen} onClose={onClose}>
      <ModalOverlay />
      <ModalContent>
        <ModalHeader fontWeight="500">Connect to MCP Server</ModalHeader>
        <ModalCloseButton />
        <ModalBody pt="2" pb="6" alignItems="stretch">
          <McpConnectInstructions userStr={userStr || "<your_username>"} />
        </ModalBody>
      </ModalContent>
    </Modal>
  );
};

export default McpConnectModal;
