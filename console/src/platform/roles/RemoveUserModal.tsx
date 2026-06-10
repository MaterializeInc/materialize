// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Button,
  HStack,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";

import Alert from "~/components/Alert";
import { Modal } from "~/components/Modal";
import { useToast } from "~/hooks/useToast";
import { MaterializeTheme } from "~/theme";

import { useRevokeRoleMember } from "./queries";

export interface RemoveUserModalProps {
  isOpen: boolean;
  onClose: () => void;
  roleName: string;
  memberName: string;
  onSuccess?: () => void;
}

const RemoveUserModal = ({
  isOpen,
  onClose,
  roleName,
  memberName,
  onSuccess,
}: RemoveUserModalProps) => {
  const { shadows, colors } = useTheme<MaterializeTheme>();
  const toast = useToast();

  const {
    mutate: revokeRole,
    isPending: isRevoking,
    error,
  } = useRevokeRoleMember();

  const handleRemove = () => {
    revokeRole(
      { roleName, memberName },
      {
        onSuccess: () => {
          onClose();
          onSuccess?.();
          toast({
            description: (
              <Text>
                <Text as="span" fontWeight="medium">
                  {memberName}
                </Text>{" "}
                removed from role
              </Text>
            ),
          });
        },
      },
    );
  };

  return (
    <Modal isOpen={isOpen} onClose={onClose} isCentered useInert={false}>
      <ModalOverlay />
      <ModalContent shadow={shadows.level4}>
        <ModalHeader p="4" borderBottom={`1px solid ${colors.border.primary}`}>
          Remove Role
        </ModalHeader>
        <ModalCloseButton />
        <ModalBody py="6">
          <VStack spacing="4" width="100%" align="start">
            {error && (
              <Alert
                variant="error"
                message={`There was an error removing the user: ${error.message}.`}
              />
            )}
            <Text textStyle="text-base" color={colors.foreground.primary}>
              Are you sure you want to remove{" "}
              <Text as="span" fontWeight="medium">
                {memberName}
              </Text>{" "}
              from this role? By doing so, that user will lose access to{" "}
              <Text as="span" fontWeight="medium">
                {roleName}
              </Text>
              .
            </Text>
          </VStack>
        </ModalBody>
        <ModalFooter>
          <HStack spacing="2" width="100%">
            <Button
              variant="secondary"
              size="sm"
              flex="1"
              onClick={onClose}
              isDisabled={isRevoking}
            >
              Cancel
            </Button>
            <Button
              colorScheme="red"
              size="sm"
              flex="1"
              onClick={handleRemove}
              isDisabled={isRevoking}
              isLoading={isRevoking}
            >
              Remove role
            </Button>
          </HStack>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
};

export default RemoveUserModal;
