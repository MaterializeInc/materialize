// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { FormControl } from "@chakra-ui/react";
import React from "react";
import { UseFormReturn } from "react-hook-form";

import Alert from "~/components/Alert";
import {
  FormContainer,
  FormSection,
  LabeledInput,
} from "~/components/formComponentsV2";
import ObjectNameInput from "~/components/ObjectNameInput";

import { CodePreviewSection } from "./CodePreviewSection";
import {
  generateCreateRoleSql,
  generateTerraformCode,
} from "./generateRoleCodePreview";
import { InheritedRolesSection } from "./InheritedRolesSection";
import { PrivilegesSection } from "./PrivilegesSection";
import { CreateRoleFormState } from "./types";

type CreateRoleFormProps = {
  form: UseFormReturn<CreateRoleFormState>;
  generalFormError?: string;
};

const RoleNameSection = ({
  form,
}: {
  form: UseFormReturn<CreateRoleFormState>;
}) => {
  const { formState, register } = form;

  return (
    <FormSection title="Configure your role" caption="Step 1">
      <FormControl isInvalid={!!formState.errors.name}>
        <LabeledInput
          label="Name"
          message="Enter a short, descriptive name for this role."
          error={formState.errors.name?.message}
        >
          <ObjectNameInput
            {...register("name", {
              required: "Role name is required.",
            })}
            placeholder="e.g. db-engineer"
            variant={formState.errors.name ? "error" : "default"}
            autoFocus
          />
        </LabeledInput>
      </FormControl>
    </FormSection>
  );
};

export const CreateRoleForm = ({
  form,
  generalFormError,
}: CreateRoleFormProps) => {
  const formValues = form.watch();
  const sqlCode = generateCreateRoleSql(formValues);
  const terraformCode = generateTerraformCode(formValues);

  return (
    <FormContainer
      aside={
        <CodePreviewSection sqlCode={sqlCode} terraformCode={terraformCode} />
      }
    >
      <RoleNameSection form={form} />
      <InheritedRolesSection form={form} />
      <PrivilegesSection form={form} />
      {generalFormError && (
        <Alert variant="error" minWidth="100%" message={generalFormError} />
      )}
    </FormContainer>
  );
};

export default CreateRoleForm;
