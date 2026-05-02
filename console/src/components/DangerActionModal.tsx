// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * @module
 * Additional modal components, beyond those provided in Chakra UI.
 */

import {
  Button,
  ButtonProps,
  HStack,
  Input,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Text,
  useDisclosure,
  VStack,
} from "@chakra-ui/react";
import React, { useState } from "react";

import { Modal } from "~/components/Modal";

interface Props extends ButtonProps {
  /** The contents of the modal. */
  children: React.ReactNode;
  /** The color scheme for the open and confirm buttons. */
  colorScheme: string;
  /** The icon to use in the open and confirm buttons. */
  confirmIcon?: React.ReactElement;
  /** The text to use in the open and confirm buttons. */
  actionText: string;
  /** The text to use in the confirm button. If unspecified, use actionText. */
  finalActionText?: string;
  /** The text the user will be required to type to confirm the action. */
  confirmText: string;
  /** The callback to invoke if the user successfully confirms the action. */
  onConfirm: () => Promise<void>;
  /** The size of the open and confirm buttons. */
  size: string;
  /** The title of the modal. */
  title: string;
}

/**
 * A modal that requires typing a prompt in order to confirm an action.
 * Intended for use with dangerous actions, like destroying a deployment.
 */
const DangerActionModal = ({
  confirmIcon,
  actionText,
  finalActionText,
  confirmText,
  onConfirm,
  children,
  title,
  ...props
}: Props) => {
  const { isOpen, onOpen, onClose } = useDisclosure();
  const [confirmation, setConfirmation] = useState("");
  const isConfirmed = confirmation === confirmText;
  const handleConfirm = async () => {
    await onConfirm();
    onClose();
  };

  return (
    <>
      <Button
        leftIcon={actionText ? confirmIcon : undefined}
        onClick={onOpen}
        title={title}
        {...props}
      >
        {actionText || confirmIcon}
      </Button>

      <Modal isOpen={isOpen} onClose={onClose}>
        <ModalOverlay />
        <ModalContent>
          <form
            onSubmit={(e) => {
              e.preventDefault();
              handleConfirm();
            }}
          >
            <ModalHeader>{title}</ModalHeader>
            <ModalCloseButton />
            <ModalBody pt="3" pb="6">
              <VStack align="left" spacing="4">
                {children}
                <VStack align="left" spacing="1">
                  <Text fontSize="sm">
                    Please type{" "}
                    <Text as="span" fontWeight="600">
                      {confirmText}
                    </Text>{" "}
                    to confirm.
                  </Text>
                  <Input
                    m="0"
                    size="sm"
                    onChange={(e) => setConfirmation(e.target.value)}
                  />
                </VStack>
              </VStack>
            </ModalBody>
            <ModalFooter>
              <HStack>
                <Button size="sm" onClick={onClose} variant="outline">
                  Cancel
                </Button>
                <Button
                  size="sm"
                  colorScheme={props.colorScheme}
                  isDisabled={!isConfirmed}
                  onClick={handleConfirm}
                >
                  {finalActionText || actionText}
                </Button>
              </HStack>
            </ModalFooter>
          </form>
        </ModalContent>
      </Modal>
    </>
  );
};

export default DangerActionModal;
