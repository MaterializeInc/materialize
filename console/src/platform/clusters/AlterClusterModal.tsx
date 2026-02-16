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
  HStack,
  Input,
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
import { Cluster } from "~/api/materialize/cluster/clusterList";
import Alert from "~/components/Alert";
import { InlineLabeledInput } from "~/components/formComponents";
import { Modal } from "~/components/Modal";
import ObjectNameInput from "~/components/ObjectNameInput";
import SimpleSelect from "~/components/SimpleSelect";
import TextLink from "~/components/TextLink";
import { ObjectToastDescription } from "~/components/Toast";
import { useToast } from "~/hooks/useToast";
import docUrls from "~/mz-doc-urls.json";
import { MaterializeTheme } from "~/theme";
import { pluralize } from "~/util";

import { useAlterCluster } from "./queries";
import { replicasErrorMessage } from "./validation";

type FormState = {
  name: string;
  size: string;
  replicas: string;
};

export const AlterClusterModal = ({
  cluster,
  clusterSizes,
  isOpen,
  maxReplicas,
  onClose,
}: {
  cluster: Cluster;
  clusterSizes: string[];
  isOpen: boolean;
  maxReplicas: number | undefined;
  onClose: () => void;
}) => {
  const { track } = useSegment();
  const [generalFormError, setGeneralFormError] = React.useState<
    string | undefined
  >(undefined);
  const toast = useToast();
  const { shadows } = useTheme<MaterializeTheme>();

  const {
    register,
    handleSubmit: handleSubmit,
    reset: formReset,
    formState,
    watch,
  } = useForm<FormState>({
    mode: "onTouched",
    defaultValues: {
      name: cluster?.name,
      size: cluster?.size || undefined,
      replicas: cluster?.replicas.length.toString(),
    },
  });

  const [size, replicas] = watch(["size", "replicas"]);

  const { mutate: alterCluster, isPending: isAlterInFlight } =
    useAlterCluster();

  const handleValidSubmit = async (values: FormState) => {
    setGeneralFormError(undefined);
    alterCluster(
      {
        ...values,
        replicas: parseInt(values.replicas, 10),
        clusterName: cluster.name,
      },
      {
        onSuccess: () => {
          onClose();
          toast({
            description: (
              <ObjectToastDescription
                name={cluster.name}
                message="altered successfully"
              />
            ),
          });
          formReset();
        },
        onError: (error) => setGeneralFormError(error.message),
      },
    );
  };

  let warning = null;
  if (replicas === "0") {
    // The user is asking for zero replicas. Warn them that this will deactive
    // the cluster, unless the cluster already has zero replicas.
    if (cluster?.replicas.length !== 0) {
      warning = "Setting replicas to 0 will deactivate the cluster.";
    }
  } else if (size !== cluster?.size) {
    // The cluster has at least one replica, and the user wants to change its
    // size. Warn them that this will result in unavailability until the new
    // replicas catch up.
    warning =
      "Changing the cluster's size will cause the cluster to be temporarily unavailable while the " +
      pluralize(
        parseInt(replicas, 10),
        "replica of the new size catches up.",
        "replicas of the new size catch up.",
      );
  }

  return (
    <Modal isOpen={isOpen} onClose={onClose}>
      <ModalOverlay />
      <ModalContent shadow={shadows.level4}>
        <form onSubmit={handleSubmit(handleValidSubmit)}>
          <ModalHeader>Alter cluster</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <VStack pb={6} spacing="4">
              {generalFormError && (
                <Alert
                  variant="error"
                  message={generalFormError}
                  width="100%"
                />
              )}
              <FormControl isInvalid={!!formState.errors.name}>
                <InlineLabeledInput
                  label="Name"
                  error="Cluster name is required."
                >
                  <ObjectNameInput
                    {...register("name", {
                      required: true,
                    })}
                    autoFocus
                    placeholder="my_production_cluster"
                    autoCorrect="off"
                    size="sm"
                  />
                </InlineLabeledInput>
              </FormControl>
              <FormControl isInvalid={!!formState.errors.size}>
                <InlineLabeledInput label="Size">
                  <SimpleSelect
                    {...register("size")}
                    width="100%"
                    defaultValue={formState.defaultValues?.size}
                  >
                    {clusterSizes?.map((sz) => (
                      <option key={sz} value={sz}>
                        {sz}
                      </option>
                    ))}
                  </SimpleSelect>
                </InlineLabeledInput>
              </FormControl>
              <FormControl isInvalid={!!formState.errors.replicas}>
                <InlineLabeledInput
                  label="Replicas"
                  error={replicasErrorMessage(
                    formState.errors.replicas,
                    maxReplicas ?? 5,
                  )}
                  message={
                    <Text>
                      Replicas provide fault tolerance, but do not improve
                      performance.{" "}
                      <TextLink
                        target="_blank"
                        href={`${docUrls["/docs/sql/create-cluster/"]}#replication-factor`}
                      >
                        View the documentation to learn more.
                      </TextLink>
                    </Text>
                  }
                >
                  <Input
                    {...register("replicas", {
                      required: true,
                      min: 0,
                      max: 5,
                    })}
                    type="number"
                    defaultValue={formState.defaultValues?.replicas}
                    min={0}
                    max={maxReplicas!}
                    size="sm"
                    width="100%"
                  />
                </InlineLabeledInput>
              </FormControl>
              {warning && (
                <Alert variant="warning" message={warning} width="100%" />
              )}
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
                isDisabled={isAlterInFlight}
                onClick={() => track("Alter Cluster Clicked")}
              >
                Alter cluster
              </Button>
            </HStack>
          </ModalFooter>
        </form>
      </ModalContent>
    </Modal>
  );
};
