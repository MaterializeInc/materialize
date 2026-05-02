// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
  Button,
  Code,
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

import DatabaseError from "~/api/materialize/DatabaseError";
import { ErrorCode } from "~/api/materialize/types";
import Alert from "~/components/Alert";
import { Modal } from "~/components/Modal";
import { useToast } from "~/hooks/useToast";
import { MaterializeTheme } from "~/theme";
import { pluralize } from "~/util";

import { useDropRole } from "./queries";

interface DropRoleWarningsProps {
  roleName: string;
  error: Error | null;
  ownedObjectsCount: number;
  isDependencyError: boolean;
}

const DropRoleWarnings = ({
  roleName,
  error,
  ownedObjectsCount,
  isDependencyError,
}: DropRoleWarningsProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const hasOwnedObjects = ownedObjectsCount > 0;
  const showInstructions = hasOwnedObjects || isDependencyError;

  return (
    <>
      {error && (
        <Alert variant="error" showLabel={false} message={error.message} />
      )}

      {!showInstructions && (
        <Text textStyle="text-base" color={colors.foreground.primary}>
          Are you sure you want to drop the role{" "}
          <Text as="span" fontWeight="medium">
            {roleName}
          </Text>
          ? This action cannot be undone.
        </Text>
      )}

      {hasOwnedObjects && (
        <Alert
          variant="warning"
          showLabel={false}
          message={
            <Text as="span" textStyle="text-ui-reg">
              This role owns {ownedObjectsCount}{" "}
              {pluralize(ownedObjectsCount, "object", "objects")}.
            </Text>
          }
        />
      )}

      {showInstructions && (
        <Box width="100%">
          <Text
            textStyle="text-small"
            color={colors.foreground.secondary}
            mb="2"
          >
            To drop this role, you need to:
          </Text>
          <VStack align="start" spacing="3">
            <Box>
              <Text
                textStyle="text-small"
                color={colors.foreground.secondary}
                mb="1"
              >
                1. Reassign or drop owned objects:
              </Text>
              <Code display="block" p="2" fontSize="xs" width="100%">
                REASSIGN OWNED BY {roleName} TO {"<new_owner>"};
              </Code>
              <Text
                textStyle="text-small"
                color={colors.foreground.secondary}
                mt="1"
              >
                or
              </Text>
              <Code display="block" p="2" fontSize="xs" width="100%" mt="1">
                DROP OWNED BY {roleName} CASCADE;
              </Code>
            </Box>
            <Box>
              <Text
                textStyle="text-small"
                color={colors.foreground.secondary}
                mb="1"
              >
                2. Revoke privileges using Edit Role
              </Text>
            </Box>
          </VStack>
        </Box>
      )}
    </>
  );
};

export interface DropRoleModalProps {
  isOpen: boolean;
  onClose: () => void;
  roleName: string;
  /** Owned objects count from existing subscription - used to show warning */
  ownedObjectsCount?: number;
  onSuccess?: () => void;
}

const DropRoleModal = ({
  isOpen,
  onClose,
  roleName,
  ownedObjectsCount = 0,
  onSuccess,
}: DropRoleModalProps) => {
  const { shadows, colors } = useTheme<MaterializeTheme>();
  const toast = useToast();

  const {
    mutate: dropRole,
    isPending: isDropping,
    error,
    reset,
  } = useDropRole();

  const hasOwnedObjects = ownedObjectsCount > 0;

  const handleDrop = () => {
    dropRole(
      { roleName },
      {
        onSuccess: () => {
          onClose();
          onSuccess?.();
          toast({
            description: (
              <Text>
                Role{" "}
                <Text as="span" fontWeight="medium">
                  {roleName}
                </Text>{" "}
                dropped successfully
              </Text>
            ),
          });
        },
      },
    );
  };

  const handleClose = () => {
    reset();
    onClose();
  };

  // Check if error is a dependency error using the proper PostgreSQL error code
  const isDependencyError =
    error instanceof DatabaseError &&
    error.details.error.code === ErrorCode.DEPENDENT_OBJECTS_STILL_EXIST;

  return (
    <Modal isOpen={isOpen} onClose={handleClose} isCentered useInert={false}>
      <ModalOverlay />
      <ModalContent shadow={shadows.level4} maxW="500px">
        <ModalHeader p="4" borderBottom={`1px solid ${colors.border.primary}`}>
          Drop Role
        </ModalHeader>
        <ModalCloseButton />
        <ModalBody py="6">
          <VStack spacing="4" width="100%" align="start">
            <DropRoleWarnings
              roleName={roleName}
              error={error}
              ownedObjectsCount={ownedObjectsCount}
              isDependencyError={isDependencyError}
            />
          </VStack>
        </ModalBody>
        <ModalFooter>
          <HStack spacing="2" width="100%">
            <Button
              variant="secondary"
              size="sm"
              flex="1"
              onClick={handleClose}
            >
              {hasOwnedObjects || isDependencyError ? "Close" : "Cancel"}
            </Button>
            {!hasOwnedObjects && !isDependencyError && (
              <Button
                colorScheme="red"
                size="sm"
                flex="1"
                onClick={handleDrop}
                isLoading={isDropping}
              >
                Drop role
              </Button>
            )}
          </HStack>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
};

export default DropRoleModal;
