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
  chakra,
  ModalBody,
  ModalContent,
  useDisclosure,
  useModalContext,
} from "@chakra-ui/react";
import React, { useMemo, useState } from "react";
import { useForm } from "react-hook-form";
import { useLocation, useNavigate } from "react-router-dom";

import { useSegment } from "~/analytics/segment";
import { queryBuilder } from "~/api/materialize";
import {
  createWebhookSourceViewStatement,
  extractKeyPaths,
} from "~/api/materialize/source/createWebhookSourceView";
import { fetchSourceList, Source } from "~/api/materialize/source/sourceList";
import ConfirmationDialog from "~/components/ConfirmationDialog";
import DeleteObjectModal from "~/components/DeleteObjectModal";
import {
  FormBottomBar,
  FormTopBar,
  WizardStep,
} from "~/components/formComponentsV2";
import { Modal } from "~/components/Modal";
import { ObjectToastDescription } from "~/components/Toast";
import { useToast } from "~/hooks/useToast";
import { useBuildSourcePath } from "~/platform/routeHelpers";
import {
  sourceQueryKeys,
  useCreateWebhookSource,
  useCreateWebhookSourceView,
} from "~/platform/sources/queries";
import { assert } from "~/util";

import useNormalizedSteps from "./shared/useNormalizedSteps";
import NewWebhookSourceForm, {
  WebhookFormState,
} from "./webhook/NewWebhookSourceForm";
import Tester from "./webhook/Tester";

function getCheckStatementError(err: Error): string | undefined {
  if (err.message === "function constant_time_eq() does not exist") {
    return "'constant_time_eq()' must receive two byte arrays";
  }
  return undefined;
}

const SOURCE_STEPS = [
  { id: "configure", label: "Configure source" },
  { id: "test", label: "Test source" },
];

export const NewWebhookSourceContent = ({
  initialSteps = [],
}: {
  initialSteps?: WizardStep[];
}) => {
  const navigate = useNavigate();
  const { track } = useSegment();
  const toast = useToast();
  const sourcePath = useBuildSourcePath();
  const {
    isOpen: isDropAlertOpen,
    onOpen: onDropAlertOpen,
    onClose: onDropAlertClose,
  } = useDisclosure();
  const { isNormalizedStep, steps, wizardSteps } = useNormalizedSteps({
    initialSteps,
    sourceSteps: SOURCE_STEPS,
  });
  const {
    isOpen: isDirtyAlertOpen,
    onOpen: onDirtyAlertOpen,
    onClose: onDirtyAlertClose,
  } = useDisclosure();
  const { onClose: onSourceModalClose } = useModalContext();

  const [generalFormError, setGeneralFormError] = useState<
    string | undefined
  >();
  const [viewCreationError, setViewCreationError] = useState<
    string | undefined
  >();
  const [createdSource, setCreatedSource] = useState<Source | null>(null);
  const form = useForm<WebhookFormState>({
    defaultValues: {
      name: "",
      schema: null,
      cluster: null,
      bodyFormat: "bytes",
      headerBehavior: "all",
      headerColumns: [{ header: "", column: "" }],
      requestValidationBehavior: "validate",
      requestCheckStatement: "",
      noValidationConfirmed: false,
    },
    mode: "onTouched",
  });
  const { mutateAsync: createWebhookSource } = useCreateWebhookSource();

  const {
    mutateAsync: createWebhookSourceView,
    isPending: isViewCreationPending,
  } = useCreateWebhookSourceView();

  const [isPending, setIsPending] = useState(false);
  const [viewName, setViewName] = useState<string | null>(null);
  const [sampleObject, setSampleObject] = useState<object | null>(null);
  const viewDdl = useMemo(() => {
    if (viewName === null || sampleObject === null || createdSource === null) {
      return null;
    }
    const columns = extractKeyPaths(sampleObject);
    const query = createWebhookSourceViewStatement(
      columns,
      viewName,
      createdSource,
      "view",
    );
    const compiled = query.compile(queryBuilder);
    return compiled.sql;
  }, [createdSource, viewName, sampleObject]);

  const handleViewUpdate = (name: string | null, sampleObj: object | null) => {
    setViewName(name);
    setSampleObject(sampleObj);
  };

  const handleValidSubmit = async (state: WebhookFormState) => {
    setGeneralFormError(undefined);
    setIsPending(true);
    try {
      if (!state.schema) {
        throw new Error("Please select a schema");
      }
      if (!state.cluster) {
        throw new Error("Please select a cluster");
      }
      await createWebhookSource({
        name: state.name,
        databaseName: state.schema.databaseName,
        schemaName: state.schema.name,
        cluster: state.cluster,
        bodyFormat: state.bodyFormat,
        headerBehavior: state.headerBehavior,
        headers: state.headerColumns,
        validateRequests: state.requestValidationBehavior === "validate",
        checkStatement: state.requestCheckStatement,
      });

      const created = await fetchSourceList({
        parameters: {
          filters: {
            nameFilter: state.name,
            databaseId: state.schema.databaseId,
            schemaId: state.schema.id,
            type: "webhook",
          },
        },
        queryKey: sourceQueryKeys.webhookSources(),
        requestOptions: {},
      });
      if (created.rows) {
        setCreatedSource(created.rows[0]);
        wizardSteps.goToNext();
        track("Created webhook source");
      }
    } catch (err: unknown) {
      if (err instanceof Error) {
        const checkErr = getCheckStatementError(err);
        if (checkErr) {
          form.setError("requestCheckStatement", {
            type: "custom",
            message: checkErr,
          });
        } else {
          setGeneralFormError(err.message);
        }
      } else if (typeof err === "string") {
        setGeneralFormError(err);
      } else {
        console.error("Failed to create webhook source");
        setGeneralFormError(
          "There was an error creating the webhook source. Please try again.",
        );
      }
    } finally {
      setIsPending(false);
    }
  };

  const stepIsValid = isNormalizedStep(0) ? form.formState.isValid : true;

  const goToNext = async () => {
    if (wizardSteps.activeStep === steps.length - 1) {
      assert(createdSource !== null);
      if (
        form.getValues("bodyFormat").startsWith("json") &&
        viewName !== null &&
        sampleObject !== null
      ) {
        try {
          setViewCreationError(undefined);
          await createWebhookSourceView({
            source: createdSource,
            viewName,
            sampleObject,
          });
          toast({
            description: (
              <ObjectToastDescription
                name={viewName}
                message="created successfully"
              />
            ),
          });
        } catch (err: unknown) {
          if (err instanceof Error) {
            setViewCreationError(err.message);
          } else if (typeof err === "string") {
            setViewCreationError(err);
          } else {
            console.error("Unhandled view creation error", err);
            setViewCreationError("unhandled error");
          }
          return;
        }
      }
      track("Finished creating webhook source", {
        createdView: viewName !== null && sampleObject !== null,
      });
      navigate(sourcePath(createdSource));
    } else {
      wizardSteps.goToNext();
    }
  };

  const getHeaderColumns = () => {
    if (form.getValues("headerBehavior") === "include_specific") {
      return form.getValues("headerColumns");
    } else if (form.getValues("headerBehavior") === "none") {
      return [];
    }
    return [{ column: "headers", header: "headers" }];
  };

  const goToPrevious = () => {
    if (isNormalizedStep(1)) {
      onDropAlertOpen();
    } else if (isNormalizedStep(0)) {
      if (Object.keys(form.formState.dirtyFields).length > 0) {
        onDirtyAlertOpen();
      } else if (wizardSteps.activeStep !== 0) {
        navigate("..");
      } else {
        onSourceModalClose();
      }
    } else {
      wizardSteps.goToPrevious();
    }
  };
  return (
    <>
      <chakra.form
        display="contents"
        onSubmit={form.handleSubmit(handleValidSubmit)}
      >
        <FormTopBar
          title="Create a Webhook Source"
          steps={steps}
          {...wizardSteps}
        />
        <ModalBody>
          {isNormalizedStep(0) && (
            <NewWebhookSourceForm
              generalFormError={generalFormError}
              form={form}
            />
          )}
          {isNormalizedStep(1) && createdSource && (
            <Box maxWidth="624px" margin="0 auto">
              <Tester
                source={createdSource}
                bodyFormat={form.getValues("bodyFormat")}
                headerColumns={getHeaderColumns()}
                viewDdl={viewDdl}
                onViewUpdate={handleViewUpdate}
                viewCreationError={viewCreationError}
              />
            </Box>
          )}
        </ModalBody>
        <FormBottomBar
          steps={steps}
          isValid={stepIsValid}
          isPending={isPending || isViewCreationPending}
          submitMessage="Finalize source"
          advanceType={isNormalizedStep(0) ? "submit" : "button"}
          {...wizardSteps}
          goToNext={goToNext}
          goToPrevious={goToPrevious}
        />
      </chakra.form>
      {createdSource !== null && (
        <DeleteObjectModal
          message={`Sources cannot be edited, so to change ${createdSource.name}'s configuration, it will first need to be dropped. This will also drop all dependent objects.`}
          isOpen={isDropAlertOpen}
          onClose={onDropAlertClose}
          onSuccess={() => {
            setCreatedSource(null);
            wizardSteps.goToPrevious();
          }}
          dbObject={createdSource}
          objectType="SOURCE"
        />
      )}
      <ConfirmationDialog
        isOpen={isDirtyAlertOpen}
        onClose={onDirtyAlertClose}
        onConfirm={onSourceModalClose}
        title="Discard changes?"
        confirmLabel="Go back"
      >
        Going back will discard any changes you have made to the form. This
        action cannot be undone.
      </ConfirmationDialog>
    </>
  );
};

const NewWebhookSource = () => {
  const navigate = useNavigate();
  const { state: locationState } = useLocation();
  const previousPage = locationState?.previousPage ?? "../connection";

  return (
    <Modal
      isOpen
      onClose={() => navigate(previousPage)}
      size="full"
      closeOnEsc={false}
    >
      <ModalContent>
        <NewWebhookSourceContent />
      </ModalContent>
    </Modal>
  );
};

export default NewWebhookSource;
