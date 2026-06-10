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
  ModalBody,
  ModalContent,
  Text,
  useSteps,
} from "@chakra-ui/react";
import React, { useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";

import {
  FormBottomBar,
  FormTopBar,
  WizardStep,
} from "~/components/formComponentsV2";
import { Modal } from "~/components/Modal";
import { useToast } from "~/hooks/useToast";
import { useCreateRole } from "~/platform/roles/queries";

import CreateRoleForm from "./CreateRoleForm";
import { useCreateRoleForm } from "./useCreateRoleForm";

const ROLE_STEPS: WizardStep[] = [{ id: "configure", label: "Configure role" }];

export const CreateRoleModal = () => {
  const navigate = useNavigate();
  const { state: locationState } = useLocation();
  const previousPage =
    (locationState as { previousPage?: string })?.previousPage ?? "..";

  const form = useCreateRoleForm();
  const createRole = useCreateRole();
  const toast = useToast();
  const [generalFormError, setGeneralFormError] = useState<string>();

  const wizardSteps = useSteps({ index: 0, count: ROLE_STEPS.length });

  const handleClose = () => {
    form.reset();
    setGeneralFormError(undefined);
    navigate(previousPage);
  };

  const handleSubmit = form.handleSubmit(async (data) => {
    setGeneralFormError(undefined);

    try {
      const result = await createRole.mutateAsync(data);

      if (result.failed.length === 0) {
        handleClose();
        toast({
          description: (
            <Text>
              The{" "}
              <Text as="span" fontWeight="medium">
                {data.name}
              </Text>{" "}
              role was successfully created
            </Text>
          ),
        });
        return;
      }

      // Partial failure: role was created but some grants failed
      const failureMessages = result.failed
        .map((f) => `${f.description}: ${f.error.errorMessage}`)
        .join("; ");

      const errorMessage =
        result.succeeded.length > 0
          ? `Role created but ${result.failed.length} grant(s) failed: ${failureMessages}`
          : `Role created but all grants failed: ${failureMessages}`;

      setGeneralFormError(errorMessage);
    } catch (error) {
      setGeneralFormError(
        error instanceof Error ? error.message : "Failed to create role",
      );
    }
  });

  return (
    <Modal
      isOpen
      onClose={handleClose}
      size="full"
      closeOnEsc={false}
      closeOnOverlayClick={false}
      scrollBehavior="inside"
    >
      <ModalContent>
        <chakra.form display="contents" onSubmit={handleSubmit}>
          <FormTopBar title="Create Role" steps={ROLE_STEPS} {...wizardSteps} />
          <ModalBody>
            <CreateRoleForm form={form} generalFormError={generalFormError} />
          </ModalBody>
          <FormBottomBar
            steps={ROLE_STEPS}
            isPending={createRole.isPending}
            isValid={form.formState.isValid}
            submitMessage="Create role"
            advanceType="submit"
            {...wizardSteps}
            goToPrevious={handleClose}
          />
        </chakra.form>
      </ModalContent>
    </Modal>
  );
};

export default CreateRoleModal;
