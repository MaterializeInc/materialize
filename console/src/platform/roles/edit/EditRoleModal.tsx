// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  chakra,
  FormControl,
  ModalBody,
  ModalContent,
  Text,
  useSteps,
} from "@chakra-ui/react";
import React, { useMemo, useState } from "react";
import { useForm } from "react-hook-form";
import { Navigate, useNavigate, useParams } from "react-router-dom";

import { ExistingRoleState } from "~/api/materialize/roles/editRole";
import Alert from "~/components/Alert";
import {
  FormBottomBar,
  FormContainer,
  FormSection,
  FormTopBar,
  LabeledInput,
  WizardStep,
} from "~/components/formComponentsV2";
import { LoadingContainer } from "~/components/LoadingContainer";
import { Modal } from "~/components/Modal";
import ObjectNameInput from "~/components/ObjectNameInput";
import { useToast } from "~/hooks/useToast";
import { CodePreviewSection } from "~/platform/roles/create/CodePreviewSection";
import {
  ObjectTypeId,
  PrivilegeGrant,
} from "~/platform/roles/create/constants";
import { InheritedRolesSection } from "~/platform/roles/create/InheritedRolesSection";
import { PrivilegesSection } from "~/platform/roles/create/PrivilegesSection";
import { CreateRoleFormState } from "~/platform/roles/create/types";
import {
  useEditRole,
  useGrantedRoles,
  useRolePrivileges,
} from "~/platform/roles/queries";

import {
  generateEditRoleSql,
  generateEditRoleTerraform,
} from "./generateEditRolePreview";

const EDIT_ROLE_STEPS: WizardStep[] = [{ id: "configure", label: "Edit role" }];

/**
 * Transform API privileges response into PrivilegeGrant format.
 * Groups privileges by object and filters to only direct privileges (where grantee === roleName).
 */
function transformPrivilegesToFormState(
  privileges: Array<{
    grantor: string;
    grantee: string;
    database: string;
    schema: string;
    name: string;
    object_type: string;
    privilege_type: string;
  }>,
  roleName: string,
): PrivilegeGrant[] {
  // Filter to direct privileges only (not inherited)
  const directPrivileges = privileges.filter((p) => p.grantee === roleName);

  // Group by object
  const byObject = new Map<string, PrivilegeGrant>();

  for (const priv of directPrivileges) {
    const objectType = priv.object_type
      .toLowerCase()
      .replace(/ /g, "-") as ObjectTypeId;

    const key = `${objectType}:${priv.database}:${priv.schema}:${priv.name}`;
    const existing = byObject.get(key);

    if (existing) {
      existing.privileges.push(priv.privilege_type);
    } else {
      byObject.set(key, {
        objectType,
        objectId: priv.name,
        objectName: priv.name,
        databaseName: priv.database || undefined,
        schemaName: priv.schema || undefined,
        privileges: [priv.privilege_type],
      });
    }
  }

  return Array.from(byObject.values());
}

export const EditRoleModal = () => {
  const navigate = useNavigate();
  const { roleName = "" } = useParams<{ roleName: string }>();
  const editRole = useEditRole();
  const toast = useToast();
  const [generalFormError, setGeneralFormError] = useState<string>();

  const wizardSteps = useSteps({ index: 0, count: EDIT_ROLE_STEPS.length });

  // Fetch existing role data
  const {
    data: grantedRolesData,
    isLoading: isLoadingGrantedRoles,
    isError: isErrorGrantedRoles,
  } = useGrantedRoles({ roleName });

  const {
    data: privilegesData,
    isLoading: isLoadingPrivileges,
    isError: isErrorPrivileges,
  } = useRolePrivileges({ roleName });

  const isLoading = isLoadingGrantedRoles || isLoadingPrivileges;
  const isError = isErrorGrantedRoles || isErrorPrivileges;

  // Compute existing state for diff calculation
  const existingState = useMemo<ExistingRoleState | null>(() => {
    if (!grantedRolesData || !privilegesData) return null;

    const inheritedRoles =
      grantedRolesData.rows?.map((row) => ({
        id: row.roleName,
        name: row.roleName,
      })) ?? [];

    const privileges = transformPrivilegesToFormState(
      privilegesData.rows ?? [],
      roleName,
    );

    return { inheritedRoles, privileges };
  }, [grantedRolesData, privilegesData, roleName]);

  // Initialize form with existing data using values prop for external sync
  const form = useForm<CreateRoleFormState>({
    mode: "onTouched",
    values: existingState
      ? {
          name: roleName,
          inheritedRoles: existingState.inheritedRoles,
          privileges: existingState.privileges,
        }
      : {
          name: roleName,
          inheritedRoles: [],
          privileges: [],
        },
  });

  const formValues = form.watch();

  const handleClose = () => {
    form.reset();
    setGeneralFormError(undefined);
    navigate("../..");
  };

  const handleSubmit = form.handleSubmit(async (data) => {
    if (!existingState) return;

    setGeneralFormError(undefined);

    try {
      const result = await editRole.mutateAsync({
        roleName,
        existingState,
        newState: data,
      });

      if (result.failed.length === 0) {
        handleClose();
        toast({
          description: (
            <Text>
              The{" "}
              <Text as="span" fontWeight="medium">
                {roleName}
              </Text>{" "}
              role was successfully updated
            </Text>
          ),
        });
        return;
      }

      // Partial failure
      const failureMessages = result.failed
        .map((f) => `${f.description}: ${f.error.errorMessage}`)
        .join("; ");

      const errorMessage =
        result.succeeded.length > 0
          ? `Some changes applied but ${result.failed.length} operation(s) failed: ${failureMessages}`
          : `All operations failed: ${failureMessages}`;

      setGeneralFormError(errorMessage);
    } catch (error) {
      setGeneralFormError(
        error instanceof Error ? error.message : "Failed to update role",
      );
    }
  });

  if (!roleName) {
    return <Navigate to="../.." replace />;
  }

  return (
    <Modal
      isOpen
      onClose={handleClose}
      size="full"
      closeOnEsc={false}
      scrollBehavior="inside"
    >
      <ModalContent>
        <chakra.form display="contents" onSubmit={handleSubmit}>
          <FormTopBar
            title={`Edit Role: ${roleName}`}
            steps={EDIT_ROLE_STEPS}
            {...wizardSteps}
          />
          <ModalBody>
            {isLoading ? (
              <LoadingContainer />
            ) : isError ? (
              <Alert
                variant="error"
                message="Failed to load role data. Please try again."
              />
            ) : (
              <FormContainer
                aside={
                  <CodePreviewSection
                    sqlCode={generateEditRoleSql(
                      roleName,
                      existingState,
                      formValues,
                    )}
                    terraformCode={generateEditRoleTerraform(
                      roleName,
                      existingState,
                      formValues,
                    )}
                    showEmptyTerraformState={false}
                  />
                }
              >
                <FormSection title="Role name" caption="Step 1">
                  <FormControl>
                    <LabeledInput
                      label="Name"
                      message="Role names cannot be changed after creation."
                    >
                      <ObjectNameInput
                        value={roleName}
                        isDisabled
                        variant="default"
                      />
                    </LabeledInput>
                  </FormControl>
                </FormSection>
                <InheritedRolesSection form={form} />
                <PrivilegesSection form={form} />
                {generalFormError && (
                  <Alert
                    variant="error"
                    minWidth="100%"
                    message={generalFormError}
                  />
                )}
              </FormContainer>
            )}
          </ModalBody>
          <FormBottomBar
            steps={EDIT_ROLE_STEPS}
            isPending={editRole.isPending || isLoading}
            isValid={form.formState.isValid && !isLoading && !isError}
            submitMessage="Update role"
            advanceType="submit"
            {...wizardSteps}
            goToPrevious={handleClose}
          />
        </chakra.form>
      </ModalContent>
    </Modal>
  );
};

export default EditRoleModal;
