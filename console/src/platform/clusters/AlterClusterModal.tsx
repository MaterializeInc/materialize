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
  Checkbox,
  Divider,
  Flex,
  FormControl,
  HStack,
  Input,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Spinner,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { useForm } from "react-hook-form";

import { useSegment } from "~/analytics/segment";
import { alterClusterStatements } from "~/api/materialize/cluster/alterCluster";
import { Cluster } from "~/api/materialize/cluster/clusterList";
import Alert from "~/components/Alert";
import { CodeBlock } from "~/components/copyableComponents";
import { InlineLabeledInput } from "~/components/formComponents";
import { Modal } from "~/components/Modal";
import ObjectNameInput from "~/components/ObjectNameInput";
import SimpleSelect from "~/components/SimpleSelect";
import TextLink from "~/components/TextLink";
import { ObjectToastDescription } from "~/components/Toast";
import { useToast } from "~/hooks/useToast";
import docUrls from "~/mz-doc-urls.json";
import { useEnvironmentGate } from "~/store/environments";
import { MaterializeTheme } from "~/theme";
import { pluralize } from "~/util";

import {
  autoScalingChange,
  AutoScalingFormValues,
  defaultBurstSize,
  lingerInputValue,
  parseLinger,
} from "./autoScalingForm";
import { AutoScaling } from "./ClusterPage/Configuration/clusterConfigurationModel";
import { AUTO_SCALING_DOCS } from "./constants";
import {
  AlterClusterParams,
  useAlterCluster,
  useClusterAutoScalingPolicy,
} from "./queries";
import { replicasErrorMessage } from "./validation";

/** First version whose ALTER CLUSTER resizes without downtime by default. */
const GRACEFUL_RESIZE_MIN_VERSION = "26.35.0";

type FormState = AutoScalingFormValues & {
  name: string;
  size: string;
  replicas: string;
};

export interface AlterClusterModalProps {
  cluster: Cluster;
  clusterSizes: string[];
  isOpen: boolean;
  maxReplicas: number | undefined;
  onClose: () => void;
  /** Opens focused on the auto-scaling section rather than the name. */
  focusAutoScaling?: boolean;
}

export const AlterClusterModal = ({
  isOpen,
  onClose,
  focusAutoScaling = false,
  ...formProps
}: AlterClusterModalProps) => {
  const { shadows } = useTheme<MaterializeTheme>();
  const {
    isAvailable,
    data: autoScaling,
    isError,
  } = useClusterAutoScalingPolicy({ cluster: formProps.cluster });
  return (
    <Modal isOpen={isOpen} onClose={onClose} size="lg">
      <ModalOverlay />
      <ModalContent shadow={shadows.level4}>
        {/* The form takes its defaults once, on mount, so it waits for the policy. */}
        {isAvailable && autoScaling === undefined && !isError ? (
          <>
            <ModalHeader>Alter cluster</ModalHeader>
            <ModalCloseButton />
            <ModalBody>
              <Flex justifyContent="center" py="10">
                <Spinner />
              </Flex>
            </ModalBody>
          </>
        ) : (
          <AlterClusterForm
            {...formProps}
            onClose={onClose}
            // A policy that couldn't be read leaves the section out rather
            // than show it as off.
            autoScaling={autoScaling}
            focusAutoScaling={focusAutoScaling}
          />
        )}
      </ModalContent>
    </Modal>
  );
};

const AlterClusterForm = ({
  cluster,
  clusterSizes,
  maxReplicas,
  onClose,
  autoScaling,
  focusAutoScaling,
}: Pick<
  AlterClusterModalProps,
  "cluster" | "clusterSizes" | "maxReplicas" | "onClose"
> & {
  /** Undefined leaves out the auto-scaling section. */
  autoScaling:
    | { policy: AutoScaling["policy"]; resizeTargetSize: string | undefined }
    | undefined;
  focusAutoScaling: boolean;
}) => {
  const { track } = useSegment();
  const [generalFormError, setGeneralFormError] = React.useState<
    string | undefined
  >(undefined);
  const toast = useToast();
  const { colors } = useTheme<MaterializeTheme>();
  const autoScalingHeadingId = React.useId();
  const autoScalingDescriptionId = React.useId();
  const isResizeGraceful =
    useEnvironmentGate(GRACEFUL_RESIZE_MIN_VERSION) === true;
  // Not the replica count, which also counts a hydration burst's or a graceful
  // resize's extra replicas, so submitting it while one runs would keep them.
  const currentReplicas = cluster.replicationFactor ?? cluster.replicas.length;
  // A submit diffs against what the form opened with. The props can change
  // under an open form, and diffing against their newer values would send
  // changes the user never made.
  const [opened] = React.useState(() => ({
    size: cluster.size,
    replicas: currentReplicas,
    autoScaling,
  }));

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
      replicas: currentReplicas.toString(),
      autoScalingEnabled: !!opened.autoScaling?.policy,
      hydrationSize:
        opened.autoScaling?.policy?.hydrationSize ??
        defaultBurstSize(
          clusterSizes,
          cluster.size,
          opened.autoScaling?.resizeTargetSize,
        ),
      linger: lingerInputValue(opened.autoScaling?.policy?.lingerMs),
    },
  });

  const values = watch();
  const { replicas } = values;

  const alterParams = (formValues: FormState): AlterClusterParams => {
    const replicaCount = parseInt(formValues.replicas, 10);
    return {
      name: formValues.name,
      // NOTE: While a resize is in flight, a SET naming SIZE or REPLICATION
      // FACTOR retargets it, and naming the realized values cancels it. So
      // only a changed value is named.
      size: formValues.size === opened.size ? undefined : formValues.size,
      replicas:
        Number.isNaN(replicaCount) || replicaCount === opened.replicas
          ? undefined
          : replicaCount,
      clusterName: cluster.name,
      autoScalingPolicy:
        opened.autoScaling === undefined
          ? undefined
          : autoScalingChange(opened.autoScaling.policy, formValues),
    };
  };
  const params = alterParams(values);
  const equivalentSql = alterClusterStatements(params, params)
    .map(({ sql }) => `${sql.trim()};`)
    .join("\n\n");

  const { mutate: alterCluster, isPending: isAlterInFlight } =
    useAlterCluster();

  const handleValidSubmit = async (formValues: FormState) => {
    setGeneralFormError(undefined);
    const submitted = alterParams(formValues);
    if (alterClusterStatements(submitted, submitted).length === 0) {
      onClose();
      return;
    }
    alterCluster(submitted, {
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
    });
  };

  let notice: { variant: "warning" | "info"; message: string } | null = null;
  if (replicas === "0") {
    // The user is asking for zero replicas. Warn them that this will deactive
    // the cluster, unless the cluster already has zero replicas.
    if (params.replicas !== undefined) {
      notice = {
        variant: "warning",
        message: "Setting replicas to 0 will deactivate the cluster.",
      };
    }
  } else if (params.size !== undefined) {
    // The cluster has at least one replica, and the user wants to change its
    // size. Before graceful resizes, this results in unavailability until the
    // new replicas catch up.
    notice = isResizeGraceful
      ? {
          variant: "info",
          message:
            "The cluster keeps serving from its current replicas until replicas of the new size catch up.",
        }
      : {
          variant: "warning",
          message:
            "Changing the cluster's size will cause the cluster to be temporarily unavailable while the " +
            pluralize(
              parseInt(replicas, 10),
              "replica of the new size catches up.",
              "replicas of the new size catch up.",
            ),
        };
  }

  return (
    <form onSubmit={handleSubmit(handleValidSubmit)}>
      <ModalHeader>Alter cluster</ModalHeader>
      <ModalCloseButton />
      <ModalBody>
        <VStack pb={6} spacing="4">
          {generalFormError && (
            <Alert variant="error" message={generalFormError} width="100%" />
          )}
          <FormControl isInvalid={!!formState.errors.name}>
            <InlineLabeledInput label="Name" error="Cluster name is required.">
              <ObjectNameInput
                {...register("name", {
                  required: true,
                })}
                autoFocus={!focusAutoScaling}
                placeholder="my_production_cluster"
                autoCorrect="off"
                size="sm"
              />
            </InlineLabeledInput>
          </FormControl>
          <FormControl isInvalid={!!formState.errors.size}>
            <InlineLabeledInput label="Size">
              <SimpleSelect
                {...register("size", { deps: ["hydrationSize"] })}
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
          {notice && (
            <Alert
              variant={notice.variant}
              message={notice.message}
              width="100%"
            />
          )}
          {opened.autoScaling !== undefined && (
            <VStack
              alignItems="stretch"
              spacing="4"
              width="100%"
              role="group"
              aria-labelledby={autoScalingHeadingId}
            >
              <Divider />
              <Text id={autoScalingHeadingId} textStyle="heading-xs">
                Auto-Scaling Policy
              </Text>
              <VStack alignItems="flex-start" spacing="1">
                <Checkbox
                  {...register("autoScalingEnabled", {
                    deps: ["hydrationSize", "linger"],
                  })}
                  // Checkbox puts its own `autoFocus` on the label.
                  inputProps={{ autoFocus: focusAutoScaling }}
                  aria-describedby={autoScalingDescriptionId}
                >
                  <Text textStyle="text-ui-med">Enable auto-scaling</Text>
                </Checkbox>
                <Text
                  id={autoScalingDescriptionId}
                  textStyle="text-small"
                  color={colors.foreground.secondary}
                >
                  While the cluster has objects to hydrate, Materialize adds a
                  replica of the burst size to speed up hydration, and removes
                  it once the cluster catches up.{" "}
                  <TextLink href={AUTO_SCALING_DOCS} isExternal>
                    Learn more
                  </TextLink>
                </Text>
              </VStack>
              {values.autoScalingEnabled && (
                <>
                  <FormControl isInvalid={!!formState.errors.hydrationSize}>
                    <InlineLabeledInput
                      label="Burst size"
                      error={formState.errors.hydrationSize?.message}
                      message="The size of the replica added while objects hydrate. Choose one larger than the cluster's size."
                    >
                      <SimpleSelect
                        {...register("hydrationSize", {
                          validate: (
                            value,
                            { autoScalingEnabled, size: clusterSize },
                          ) => {
                            if (!autoScalingEnabled) return true;
                            if (value === "") return "Choose a burst size.";
                            // The planner rejects a HYDRATION SIZE equal to
                            // the cluster's SIZE.
                            if (value === clusterSize) {
                              return "Burst size must differ from the cluster's size.";
                            }
                            // An unchanged size leaves the in-progress resize
                            // running, and the server rejects a HYDRATION SIZE
                            // equal to its target.
                            if (
                              clusterSize === opened.size &&
                              value === opened.autoScaling?.resizeTargetSize
                            ) {
                              return "Burst size must differ from the size the cluster is resizing to.";
                            }
                            return true;
                          },
                        })}
                        width="100%"
                        placeholder="Select a size"
                      >
                        {clusterSizes.map((sz) => (
                          <option key={sz} value={sz}>
                            {sz}
                          </option>
                        ))}
                      </SimpleSelect>
                    </InlineLabeledInput>
                  </FormControl>
                  <FormControl isInvalid={!!formState.errors.linger}>
                    <InlineLabeledInput
                      label="Linger duration"
                      error={formState.errors.linger?.message}
                      message="How long the burst replica stays once the cluster catches up, like 15s or 2m. Leave empty for the default."
                    >
                      <Input
                        {...register("linger", {
                          validate: (value, { autoScalingEnabled }) => {
                            if (!autoScalingEnabled) return true;
                            const parsed = parseLinger(value);
                            return parsed.ok || parsed.message;
                          },
                        })}
                        placeholder="Default"
                        autoCorrect="off"
                        size="sm"
                        width="100%"
                      />
                    </InlineLabeledInput>
                  </FormControl>
                </>
              )}
            </VStack>
          )}
          <VStack alignItems="stretch" spacing="2" width="100%">
            <Divider />
            <Text textStyle="heading-xs">Equivalent code</Text>
            {equivalentSql === "" ? (
              <Text textStyle="text-small" color={colors.foreground.secondary}>
                No changes to apply.
              </Text>
            ) : (
              <CodeBlock
                title="SQL"
                contents={equivalentSql}
                lineNumbers={false}
              />
            )}
          </VStack>
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
  );
};
