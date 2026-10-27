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
  HStack,
  IconButton,
  Link,
  Tag,
  Text,
  useTheme,
  VStack,
  Wrap,
} from "@chakra-ui/react";
import React from "react";
import { useController, UseFormReturn } from "react-hook-form";

import { FormSection } from "~/components/formComponentsV2";
import SearchableSelect from "~/components/SearchableSelect/SearchableSelect";
import docUrls from "~/mz-doc-urls.json";
import { useAllRoles } from "~/store/allRoles";
import { MaterializeTheme } from "~/theme";

import { CreateRoleFormState } from "./types";

type InheritedRolesSectionProps = {
  form: UseFormReturn<CreateRoleFormState>;
};

const SYSTEM_ROLES = ["PUBLIC", "mz_system", "mz_support"];

export const InheritedRolesSection = ({ form }: InheritedRolesSectionProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { data: existingRoles } = useAllRoles();

  const { field } = useController({
    control: form.control,
    name: "inheritedRoles",
  });

  const selectedRoles = field.value || [];

  const availableRoles =
    existingRoles?.filter(
      (role) =>
        !selectedRoles.some((r) => r.name === role.roleName) &&
        !SYSTEM_ROLES.includes(role.roleName),
    ) || [];

  const handleAdd = (role: { id: string; name: string } | null) => {
    if (role) {
      field.onChange([...selectedRoles, role]);
    }
  };

  const handleRemove = (roleName: string) => {
    field.onChange(selectedRoles.filter((r) => r.name !== roleName));
  };

  return (
    <FormSection
      title="Configure privileges on this role"
      caption="Step 2"
      description=""
    >
      <VStack alignItems="flex-start" spacing={4} width="100%">
        <Box>
          <Text textStyle="text-ui-med" mb={1}>
            Inherit from{" "}
            <Text as="span" color={colors.foreground.tertiary}>
              (optional)
            </Text>
          </Text>
          <Text textStyle="text-small" color={colors.foreground.secondary}>
            Select existing roles whose privileges should be inherited by this
            role.{" "}
            <Link
              href={docUrls["/docs/sql/grant-role/"]}
              color={colors.accent.brightPurple}
              isExternal
            >
              Learn more
            </Link>{" "}
            about role inheritance.
          </Text>
        </Box>

        <Box maxWidth="320px" width="100%">
          <SearchableSelect
            ariaLabel="Select role to inherit"
            placeholder="Add role to inherit from..."
            options={availableRoles.map((role) => ({
              id: role.roleName,
              name: role.roleName,
            }))}
            value={null}
            onChange={handleAdd}
            usePortal
          />
        </Box>

        {selectedRoles.length > 0 && (
          <Wrap spacing={2}>
            {selectedRoles.map((role) => (
              <Tag key={role.name} size="md" borderRadius="md">
                <HStack spacing={1}>
                  <Text>{role.name}</Text>
                  <IconButton
                    aria-label={`Remove ${role.name}`}
                    icon={<CloseIcon boxSize={2} />}
                    size="xs"
                    variant="ghost"
                    minWidth="auto"
                    height="auto"
                    onClick={() => handleRemove(role.name)}
                  />
                </HStack>
              </Tag>
            ))}
          </Wrap>
        )}
      </VStack>
    </FormSection>
  );
};

export default InheritedRolesSection;
