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
  Checkbox,
  FormControl,
  FormLabel,
  HStack,
  IconButton,
  Link,
  ListItem,
  Text,
  UnorderedList,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { useState } from "react";
import { useController, UseFormReturn } from "react-hook-form";

import { FormSection } from "~/components/formComponentsV2";
import SearchableSelect from "~/components/SearchableSelect/SearchableSelect";
import docUrls from "~/mz-doc-urls.json";
import { MaterializeTheme } from "~/theme";

import {
  OBJECT_TYPES,
  ObjectType,
  ObjectTypeId,
  PrivilegeGrant,
  PRIVILEGES_BY_OBJECT_TYPE,
} from "./constants";
import { CreateRoleFormState } from "./types";
import { ObjectOption, useObjectsByType } from "./useObjectsByType";

const OBJECT_TYPE_NAMES: Record<ObjectTypeId, string> = Object.fromEntries(
  OBJECT_TYPES.map((t) => [t.id, t.name]),
) as Record<ObjectTypeId, string>;

const CARD_STYLES = {
  border: "1px solid",
  borderRadius: "md",
  p: 4,
  width: "100%",
  maxWidth: "600px",
};

type PrivilegesSectionProps = {
  form: UseFormReturn<CreateRoleFormState>;
};

type PrivilegeFormState = {
  objectType: ObjectType | null;
  object: ObjectOption | null;
  selectedPrivileges: string[];
};

/**
 * Inline form for adding a single privilege grant. Manages its own local state
 * for the 3-step selection: object type → object → privileges (checkboxes).
 * Calls onAdd with the completed PrivilegeGrant when submitted.
 */
const PrivilegeInlineForm = ({
  onAdd,
  onCancel,
}: {
  onAdd: (privilege: PrivilegeGrant) => void;
  onCancel: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [formState, setFormState] = useState<PrivilegeFormState>({
    objectType: null,
    object: null,
    selectedPrivileges: [],
  });

  const objectTypeId = formState.objectType?.id;
  const { objects: availableObjects, isLoading } =
    useObjectsByType(objectTypeId);

  const availablePrivileges = objectTypeId
    ? (PRIVILEGES_BY_OBJECT_TYPE[objectTypeId] ?? [])
    : [];

  const handleObjectTypeChange = (value: ObjectType | null) => {
    const isSystem = value?.id === "system";
    setFormState({
      objectType: value,
      object: isSystem ? { id: "SYSTEM", name: "SYSTEM" } : null,
      selectedPrivileges: [],
    });
  };

  const handleObjectChange = (value: ObjectOption | null) => {
    setFormState((prev) => ({
      ...prev,
      object: value,
      selectedPrivileges: [],
    }));
  };

  const togglePrivilege = (id: string, checked: boolean) => {
    setFormState((prev) => ({
      ...prev,
      selectedPrivileges: checked
        ? [...prev.selectedPrivileges, id]
        : prev.selectedPrivileges.filter((p) => p !== id),
    }));
  };

  const handleAdd = () => {
    const { objectType, object, selectedPrivileges } = formState;
    if (!objectType || !object || !selectedPrivileges.length) return;
    onAdd({
      objectType: objectType.id,
      objectId: object.id,
      objectName: object.name,
      databaseName: object.databaseName,
      schemaName: object.schemaName,
      privileges: selectedPrivileges,
    });
  };

  const isValid =
    formState.objectType &&
    formState.object &&
    formState.selectedPrivileges.length > 0;

  return (
    <Box {...CARD_STYLES} borderColor={colors.border.secondary}>
      <VStack spacing={4} alignItems="stretch">
        <HStack spacing={4} alignItems="flex-start">
          <FormControl flex={1}>
            <FormLabel fontSize="sm" fontWeight="medium">
              Object type
            </FormLabel>
            <SearchableSelect
              ariaLabel="Select object type"
              placeholder="Select..."
              options={OBJECT_TYPES}
              value={formState.objectType}
              onChange={handleObjectTypeChange}
              containerWidth="100%"
              usePortal
            />
          </FormControl>

          {formState.objectType && formState.objectType.id !== "system" && (
            <FormControl flex={1}>
              <FormLabel fontSize="sm" fontWeight="medium">
                Object
              </FormLabel>
              {isLoading ? (
                <Text
                  textStyle="text-small"
                  color={colors.foreground.secondary}
                >
                  Loading...
                </Text>
              ) : availableObjects.length > 0 ? (
                <SearchableSelect
                  ariaLabel="Select object"
                  placeholder="Select..."
                  options={availableObjects}
                  value={formState.object}
                  onChange={handleObjectChange}
                  containerWidth="100%"
                  menuWidth="350px"
                  usePortal
                />
              ) : (
                <Text
                  textStyle="text-small"
                  color={colors.foreground.secondary}
                >
                  No objects available
                </Text>
              )}
            </FormControl>
          )}
        </HStack>

        {formState.objectType && formState.object && (
          <Box>
            <Text fontSize="sm" fontWeight="medium" mb={1}>
              Privileges
            </Text>
            <Text
              textStyle="text-small"
              color={colors.foreground.secondary}
              mb={3}
            >
              Available privileges depend on the selected object type
            </Text>
            <VStack alignItems="flex-start" spacing={2}>
              {availablePrivileges.map((privilege) => (
                <Checkbox
                  key={privilege}
                  isChecked={formState.selectedPrivileges.includes(privilege)}
                  onChange={(e) => togglePrivilege(privilege, e.target.checked)}
                  size="sm"
                >
                  {privilege}
                </Checkbox>
              ))}
            </VStack>
          </Box>
        )}

        <HStack justifyContent="flex-end" spacing={2} pt={2}>
          <Button variant="outline" size="sm" onClick={onCancel}>
            Cancel
          </Button>
          <Button
            variant="primary"
            size="sm"
            onClick={handleAdd}
            isDisabled={!isValid}
          >
            Add privilege
          </Button>
        </HStack>
      </VStack>
    </Box>
  );
};

/**
 * Displays a single privilege grant as a card with object type, object name,
 * privileges list, and a remove button.
 */
const PrivilegeCard = ({
  grant,
  onRemove,
}: {
  grant: PrivilegeGrant;
  onRemove: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Box {...CARD_STYLES} borderColor={colors.border.secondary}>
      <HStack justifyContent="space-between" mb={3}>
        <HStack spacing={4}>
          <Box>
            <Text fontSize="xs" color={colors.foreground.tertiary} mb={0.5}>
              Object type
            </Text>
            <Text textStyle="text-ui-med">
              {OBJECT_TYPE_NAMES[grant.objectType] ?? grant.objectType}
            </Text>
          </Box>
          <Box>
            <Text fontSize="xs" color={colors.foreground.tertiary} mb={0.5}>
              Object
            </Text>
            <Text textStyle="text-ui-med">{grant.objectName}</Text>
          </Box>
        </HStack>
        <IconButton
          aria-label="Remove privilege"
          icon={<CloseIcon boxSize={2} />}
          size="xs"
          variant="ghost"
          onClick={onRemove}
        />
      </HStack>
      <Box>
        <Text fontSize="xs" color={colors.foreground.tertiary} mb={2}>
          Privileges
        </Text>
        <UnorderedList spacing={1} ml={4}>
          {grant.privileges.map((priv) => (
            <ListItem key={priv} textStyle="text-small">
              {priv}
            </ListItem>
          ))}
        </UnorderedList>
      </Box>
    </Box>
  );
};

/**
 * Container component for the privileges section of the create role form.
 * Displays the list of added privilege grants, handles adding/removing privileges,
 * and toggles the PrivilegeInlineForm for adding new entries.
 */
export const PrivilegesSection = ({ form }: PrivilegesSectionProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [isAdding, setIsAdding] = useState(false);

  const { field: privilegesField } = useController({
    control: form.control,
    name: "privileges",
  });

  const currentPrivileges: PrivilegeGrant[] = privilegesField.value || [];

  const handleAddPrivilege = (newGrant: PrivilegeGrant) => {
    privilegesField.onChange([...currentPrivileges, newGrant]);
    setIsAdding(false);
  };

  const handleRemovePrivilege = (index: number) => {
    privilegesField.onChange(currentPrivileges.filter((_, i) => i !== index));
  };

  return (
    <FormSection title="" caption="">
      <VStack alignItems="flex-start" spacing={4} width="100%">
        <Box>
          <Text textStyle="text-ui-med" mb={1}>
            Direct privileges
          </Text>
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            Add privileges that apply specifically to this role only.{" "}
            <Link
              href={docUrls["/docs/sql/grant-privilege/"]}
              color={colors.accent.brightPurple}
              isExternal
            >
              Learn more
            </Link>{" "}
            about direct privileges.
          </Text>
        </Box>

        {currentPrivileges.map((grant, index) => (
          <PrivilegeCard
            key={`${grant.objectType}-${grant.objectId}-${index}`}
            grant={grant}
            onRemove={() => handleRemovePrivilege(index)}
          />
        ))}

        {isAdding ? (
          <PrivilegeInlineForm
            onAdd={handleAddPrivilege}
            onCancel={() => setIsAdding(false)}
          />
        ) : (
          <Button variant="outline" size="sm" onClick={() => setIsAdding(true)}>
            + Add privilege
          </Button>
        )}
      </VStack>
    </FormSection>
  );
};

export default PrivilegesSection;
