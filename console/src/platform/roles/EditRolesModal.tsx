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
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Tag,
  Text,
  useTheme,
  VStack,
  Wrap,
} from "@chakra-ui/react";
import React, { useState } from "react";

import Alert from "~/components/Alert";
import { Modal } from "~/components/Modal";
import SearchableSelect from "~/components/SearchableSelect/SearchableSelect";
import { useToast } from "~/hooks/useToast";
import { useAllRoles } from "~/store/allRoles";
import { MaterializeTheme } from "~/theme";

import { useUpdateUserRoles } from "./queries";

export interface EditRolesModalProps {
  isOpen: boolean;
  onClose: () => void;
  userName: string;
  // The roles currently assigned to the user, can be removed or added in the edit role modal
  currentRoles: string[];
}

const SYSTEM_ROLES = ["PUBLIC", "mz_system", "mz_support"];

export const EditRolesModal = ({
  isOpen,
  onClose,
  userName,
  currentRoles,
}: EditRolesModalProps) => {
  const { shadows, colors } = useTheme<MaterializeTheme>();
  const toast = useToast();
  const { data: allRolesData } = useAllRoles();

  const [selectedRoles, setSelectedRoles] = useState<string[]>(currentRoles);
  const [error, setError] = useState<string>();

  const { mutateAsync: updateRoles, isPending: isSaving } =
    useUpdateUserRoles();

  const availableRoles =
    allRolesData?.filter(
      (role) =>
        !selectedRoles.includes(role.roleName) &&
        !SYSTEM_ROLES.includes(role.roleName),
    ) || [];

  const handleAdd = (role: { id: string; name: string } | null) => {
    if (role) {
      setSelectedRoles([...selectedRoles, role.name]);
      setError(undefined);
    }
  };

  const handleRemove = (roleName: string) => {
    setSelectedRoles(selectedRoles.filter((r) => r !== roleName));
    setError(undefined);
  };

  const handleSave = async () => {
    const rolesToGrant = selectedRoles.filter(
      (role) => !currentRoles.includes(role),
    );
    const rolesToRevoke = currentRoles.filter(
      (role) => !selectedRoles.includes(role),
    );

    if (rolesToGrant.length === 0 && rolesToRevoke.length === 0) {
      onClose();
      return;
    }

    setError(undefined);

    try {
      const { succeeded, failed } = await updateRoles({
        memberName: userName,
        rolesToGrant,
        rolesToRevoke,
      });

      if (failed.length === 0) {
        onClose();
        toast({
          description: (
            <Text>
              Roles were updated for{" "}
              <Text as="span" fontWeight="medium">
                {userName}
              </Text>
            </Text>
          ),
        });
        return;
      }

      const failureMessages = failed
        .map(
          (f) =>
            `Failed to ${f.operation} ${f.roleName}: ${f.error.errorMessage}`,
        )
        .join("; ");

      const errorMessage =
        succeeded.length > 0
          ? `Partial update: ${succeeded.length} succeeded, ${failed.length} failed. ${failureMessages}`
          : failureMessages;

      setError(errorMessage);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to update roles");
    }
  };

  return (
    <Modal isOpen={isOpen} onClose={onClose} isCentered useInert={false}>
      <ModalOverlay />
      <ModalContent shadow={shadows.level4}>
        <ModalHeader p="4" borderBottom={`1px solid ${colors.border.primary}`}>
          Edit Roles
        </ModalHeader>
        <ModalCloseButton />
        <ModalBody py="6">
          <VStack spacing="4" width="100%" align="start">
            {error && <Alert variant="error" message={error} />}

            <Text textStyle="text-base" color={colors.foreground.secondary}>
              Assign and remove roles for {userName}
            </Text>

            <Box width="100%">
              <Text textStyle="text-ui-med" mb={2}>
                Select roles
              </Text>
              <SearchableSelect
                ariaLabel="Select role to assign"
                placeholder="Add role..."
                options={availableRoles.map((role) => ({
                  id: role.roleName,
                  name: role.roleName,
                }))}
                value={null}
                onChange={handleAdd}
                isDisabled={isSaving}
              />
            </Box>

            {selectedRoles.length > 0 && (
              <Wrap spacing={2}>
                {selectedRoles.map((roleName) => (
                  <Tag key={roleName} size="md" borderRadius="md">
                    <HStack spacing={1}>
                      <Text>{roleName}</Text>
                      <IconButton
                        aria-label={`Remove ${roleName}`}
                        icon={<CloseIcon boxSize={2} />}
                        size="xs"
                        variant="ghost"
                        minWidth="auto"
                        height="auto"
                        onClick={() => handleRemove(roleName)}
                        isDisabled={isSaving}
                      />
                    </HStack>
                  </Tag>
                ))}
              </Wrap>
            )}
          </VStack>
        </ModalBody>
        <ModalFooter>
          <HStack spacing="2" width="100%">
            <Button
              variant="secondary"
              size="sm"
              flex="1"
              onClick={onClose}
              isDisabled={isSaving}
            >
              Cancel
            </Button>
            <Button
              variant="primary"
              size="sm"
              flex="1"
              onClick={handleSave}
              isDisabled={isSaving}
              isLoading={isSaving}
            >
              Save changes
            </Button>
          </HStack>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
};

export default EditRolesModal;
