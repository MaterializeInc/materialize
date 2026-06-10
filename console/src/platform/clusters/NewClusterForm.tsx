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
  Input,
  ModalContent,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { sql } from "kysely";
import React from "react";
import { useForm } from "react-hook-form";
import { useNavigate } from "react-router-dom";

import { useSegment } from "~/analytics/segment";
import {
  CATALOG_SERVER_CLUSTER,
  queryBuilder,
  useSqlLazy,
} from "~/api/materialize";
import { escapedLiteral as lit } from "~/api/materialize";
import { alreadyExistsError } from "~/api/materialize/parseErrors";
import Alert from "~/components/Alert";
import {
  FormContainer,
  FormInfoBox,
  FormSection,
  FormTopBar,
  InlineLabeledInput,
} from "~/components/formComponents";
import { Modal } from "~/components/Modal";
import ObjectNameInput from "~/components/ObjectNameInput";
import SimpleSelect from "~/components/SimpleSelect";
import TextLink from "~/components/TextLink";
import { ObjectToastDescription } from "~/components/Toast";
import { useToast } from "~/hooks/useToast";
import docUrls from "~/mz-doc-urls.json";
import { relativeClusterPath } from "~/platform/routeHelpers";
import { MaterializeTheme } from "~/theme";
import { assert, capitalizeSentence } from "~/util";

import {
  useAvailableClusterSizes,
  useClusters,
  useMaxReplicasPerCluster,
} from "./queries";

type FormState = {
  name: string;
  size: string;
  replicas: number;
};

const DEFAULT_SIZE_OPTION = "25cc";

const NewClusterForm = () => {
  const [generalFormError, setGeneralFormError] = React.useState<
    string | undefined
  >(undefined);
  const navigate = useNavigate();
  const toast = useToast();

  const { colors } = useTheme<MaterializeTheme>();
  const { track } = useSegment();

  const { data: clusterSizes } = useAvailableClusterSizes();
  const { data: maxReplicas } = useMaxReplicasPerCluster();

  const { formState, handleSubmit, register, setError, setFocus } =
    useForm<FormState>({
      defaultValues: {
        name: "",
        size: DEFAULT_SIZE_OPTION,
        replicas: 1,
      },
      mode: "onTouched",
    });

  const { refetch: refetchClusters } = useClusters();

  const { runSql: createCluster, loading: isCreating } = useSqlLazy({
    queryBuilder: ({ name, size, replicas }: FormState) => {
      const createClusterStatement = sql`CREATE CLUSTER ${sql.id(
        name,
      )} SIZE = ${lit(size)}, REPLICATION FACTOR = ${sql.raw(
        replicas.toString(),
      )}`.compile(queryBuilder);
      const selectClusterIdQuery = queryBuilder
        .selectFrom("mz_clusters")
        .where("name", "=", name)
        .select("id")
        .compile();
      return {
        queries: [
          {
            query: createClusterStatement.sql,
            params: createClusterStatement.parameters as string[],
          },
          {
            query: selectClusterIdQuery.sql,
            params: selectClusterIdQuery.parameters as string[],
          },
        ],
        cluster: CATALOG_SERVER_CLUSTER,
      };
    },
  });

  const handleError = ({
    errorMessage,
    values,
  }: {
    errorMessage?: string;
    values: FormState;
  }) => {
    const objectName = alreadyExistsError(errorMessage);
    if (objectName) {
      const userErrorMessage = capitalizeSentence(errorMessage ?? "");
      if (objectName === values.name) {
        setError("name", {
          message: userErrorMessage,
        });
        setFocus("name");
      }
    } else {
      setGeneralFormError(errorMessage);
    }
  };

  const handleValidSubmit = (values: FormState) => {
    createCluster(values, {
      onSuccess: async (response) => {
        try {
          assert(response);
          const clusterId = response[1].rows[0][0] as string;
          await refetchClusters();
          toast({
            description: (
              <ObjectToastDescription
                name={values.name}
                message="created successfully"
              />
            ),
          });
          navigate(
            `../${relativeClusterPath({ id: clusterId, name: values.name })}`,
          );
        } catch {
          navigate("..");
        }
      },
      onError: (errorMessage) => {
        handleError({ errorMessage, values });
      },
    });
  };

  return (
    <Modal isOpen onClose={() => navigate("..")} size="full" closeOnEsc={false}>
      <ModalContent>
        <form onSubmit={handleSubmit(handleValidSubmit)}>
          <FormTopBar title="New cluster" backButtonHref="..">
            <Button
              variant="primary"
              size="sm"
              type="submit"
              isDisabled={isCreating}
              onClick={() => track("Create Cluster Clicked")}
            >
              Create cluster
            </Button>
          </FormTopBar>
          <FormContainer
            title="Cluster configuration"
            aside={
              <FormInfoBox>
                <Text
                  fontSize="14px"
                  lineHeight="16px"
                  fontWeight={500}
                  color={colors.foreground.primary}
                  mb={2}
                >
                  Not sure how to configure your cluster?
                </Text>
                <Text
                  fontSize="14px"
                  lineHeight="20px"
                  color={colors.foreground.secondary}
                  maxW={{ md: "40ch" }}
                  mb={4}
                >
                  View the documentation to learn how clusters work in
                  Materialize.
                </Text>
                <TextLink
                  fontSize="14px"
                  lineHeight="16px"
                  fontWeight={500}
                  color={colors.accent.brightPurple}
                  sx={{
                    fontFeatureSettings: '"calt"',
                    textDecoration: "none",
                  }}
                  href={docUrls["/docs/sql/create-cluster/"]}
                  target="_blank"
                  rel="noopener"
                >
                  View documentation -&gt;
                </TextLink>
              </FormInfoBox>
            }
          >
            {generalFormError && (
              <Alert variant="error" message={generalFormError} mb="40px" />
            )}
            <FormSection title="General">
              <VStack spacing="6">
                <FormControl isInvalid={!!formState.errors.name}>
                  <InlineLabeledInput
                    label="Name"
                    error={formState.errors.name?.message}
                  >
                    <ObjectNameInput
                      {...register("name", {
                        required: "Cluster name is required.",
                      })}
                      autoFocus
                      placeholder="my_production_cluster"
                      autoCorrect="off"
                      size="sm"
                      variant={formState.errors.name ? "error" : "default"}
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
                      {clusterSizes?.map((size) => (
                        <option key={size} value={size}>
                          {size}
                        </option>
                      ))}
                    </SimpleSelect>
                  </InlineLabeledInput>
                </FormControl>
                <FormControl isInvalid={!!formState.errors.replicas}>
                  <InlineLabeledInput
                    label="Replicas"
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
                        required: "Replicas is required.",
                        min: 0,
                        max: maxReplicas!,
                      })}
                      type="number"
                      defaultValue={formState.defaultValues?.replicas}
                      min={0}
                      max={maxReplicas!}
                      variant="default"
                      size="sm"
                      width="100%"
                    />
                  </InlineLabeledInput>
                </FormControl>
              </VStack>
            </FormSection>
          </FormContainer>
        </form>
      </ModalContent>
    </Modal>
  );
};

export default NewClusterForm;
