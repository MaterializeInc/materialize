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
  FormControl,
  FormErrorMessage,
  FormLabel,
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
import { useForm } from "react-hook-form";

import { useSegment } from "~/analytics/segment";
import { queryBuilder, useSqlLazy } from "~/api/materialize";
import createClusterReplicaStatement from "~/api/materialize/cluster/createClusterReplicaStatement";
import { duplicateReplicaName } from "~/api/materialize/parseErrors";
import Alert from "~/components/Alert";
import { LoadingContainer } from "~/components/LoadingContainer";
import { Modal } from "~/components/Modal";
import ObjectNameInput from "~/components/ObjectNameInput";
import SimpleSelect from "~/components/SimpleSelect";
import TextLink from "~/components/TextLink";
import { useToast } from "~/hooks/useToast";
import docUrls from "~/mz-doc-urls.json";
import { MaterializeTheme } from "~/theme";

import { useAvailableClusterSizes } from "./queries";

type FormState = {
  name: string;
  size: string;
};

const NewReplicaForm = ({
  clusterName,
  onClose,
  onSubmit,
}: {
  clusterName: string;
  onClose: () => void;
  onSubmit: () => void;
}) => {
  const { track } = useSegment();
  const [generalFormError, setGeneralFormError] = React.useState<
    string | undefined
  >(undefined);
  const toast = useToast();

  const {
    register,
    handleSubmit: handleSubmit,
    reset: formReset,
    formState,
    setError,
  } = useForm<FormState>({
    mode: "onTouched",
  });

  const { data: clusterSizes } = useAvailableClusterSizes();
  const { runSql: createReplica, loading: isCreationInFlight } = useSqlLazy({
    queryBuilder: (values: FormState & { clusterName: string }) => {
      const compiledStatement =
        createClusterReplicaStatement(values).compile(queryBuilder);
      return {
        queries: [
          {
            query: compiledStatement.sql,
            params: compiledStatement.parameters as string[],
          },
        ],
        cluster: "mz_catalog_server",
      };
    },
  });

  const handleValidSubmit = async (values: FormState) => {
    setGeneralFormError(undefined);
    createReplica(
      { ...values, clusterName },
      {
        onSuccess: () => {
          onSubmit();
          toast({
            description: "New replica created successfully",
          });
          formReset();
        },
        onError: (errorMessage) => {
          const objectName = duplicateReplicaName(errorMessage);
          if (objectName === values.name) {
            setError("name", {
              message:
                "A replica with that name already exists in this cluster.",
            });
          } else {
            setGeneralFormError(errorMessage);
          }
        },
      },
    );
  };

  return (
    <form onSubmit={handleSubmit(handleValidSubmit)}>
      <ModalHeader>Create Replica</ModalHeader>
      <ModalCloseButton />
      <ModalBody>
        <VStack pb={6} spacing="4">
          <Text textStyle="text-base">
            Replicas provide fault tolerence and do not improve performance.{" "}
            <TextLink
              target="_blank"
              href={`${docUrls["/docs/sql/create-cluster/"]}#replication-factor`}
            >
              View the docs to learn more.
            </TextLink>
          </Text>
          {generalFormError && (
            <Alert variant="error" message={generalFormError} />
          )}
          <FormControl isInvalid={!!formState.errors.name}>
            <FormLabel fontSize="sm">Name</FormLabel>
            <ObjectNameInput
              {...register("name", {
                required: "Name is required.",
              })}
              placeholder="Choose something descriptive"
              autoFocus
              autoCorrect="off"
              size="sm"
              variant={formState.errors.name ? "error" : "default"}
            />
            <FormErrorMessage>
              {formState.errors.name?.message}
            </FormErrorMessage>
          </FormControl>
          <FormControl isInvalid={!!formState.errors.size}>
            <FormLabel fontSize="sm">Size</FormLabel>
            {clusterSizes && (
              <SimpleSelect {...register("size" as const)}>
                {clusterSizes.map((size) => (
                  <option key={size} value={size}>
                    {size}
                  </option>
                ))}
              </SimpleSelect>
            )}
            <FormErrorMessage>
              {formState.errors.size?.message}
            </FormErrorMessage>
          </FormControl>
        </VStack>
      </ModalBody>

      <ModalFooter>
        <HStack spacing="2">
          <Button variant="secondary" size="sm" onClick={onClose}>
            Cancel
          </Button>
          <Button
            type="submit"
            variant="primary"
            size="sm"
            isDisabled={isCreationInFlight}
            onClick={() => track("Create Replica Clicked")}
          >
            Create replica
          </Button>
        </HStack>
      </ModalFooter>
    </form>
  );
};

const NewReplicaModal = ({
  clusterName,
  isOpen,
  onClose,
  onSubmit,
}: {
  clusterName: string;
  isOpen: boolean;
  onClose: () => void;
  onSubmit: () => void;
}) => {
  const { shadows } = useTheme<MaterializeTheme>();
  return (
    <Modal isOpen={isOpen} onClose={onClose}>
      <ModalOverlay />
      <ModalContent shadow={shadows.level4}>
        <React.Suspense
          fallback={
            <ModalBody>
              <LoadingContainer />
            </ModalBody>
          }
        >
          <NewReplicaForm
            clusterName={clusterName}
            onClose={onClose}
            onSubmit={onSubmit}
          />
        </React.Suspense>
      </ModalContent>
    </Modal>
  );
};

export default NewReplicaModal;
