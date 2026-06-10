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
  chakra,
  FormControl,
  FormErrorMessage,
  FormLabel,
  HStack,
  Text,
  Textarea,
  Tooltip,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { useForm } from "react-hook-form";
import { Navigate, useNavigate } from "react-router-dom";

import { submitOnboardingSurvey } from "~/analytics/hubspot";
import {
  MATERIALIZE_USE_CASE_OPTIONS,
  MaterializeUseCase,
  OTHER_OPTION,
  PRIMARY_DATA_SOURCE_OPTIONS,
  PrimaryDataSource,
  PROJECT_DESCRIPTION_OPTIONS,
  ProjectDescription,
  ROLE_DESCRIPTION_OPTIONS,
  RoleDescription,
} from "~/analytics/onboardingSurveyOptions";
import SimpleSelect from "~/components/SimpleSelect";
import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import { User } from "~/external-library-wrappers/frontegg";
import { useEnvironmentsWithHealth } from "~/store/environments";

import ContactSalesCta from "./ContactSalesCta";
import { NAVBAR_HEIGHT } from "./Layout";

const PLACEHOLDER_TEXT = "Select...";

type FormState = {
  roleDescription: RoleDescription;
  roleDescriptionDetails?: string;
  materializeUseCase: MaterializeUseCase;
  materializeUseCaseDetails?: string;
  projectDescription: ProjectDescription;
  projectDescriptionDetails?: string;
  primaryDataSource: PrimaryDataSource;
  primaryDataSourceDetails?: string;
};

const SkipSurveyButton = ({
  onClick,
}: {
  onClick: (e: React.MouseEvent) => void;
}) => (
  <Button
    variant="secondary"
    onClick={onClick}
    title="Skip this survey (internal only)"
  >
    Skipperoni
  </Button>
);

const OnboardingSurvey = ({ user }: { user: User }) => {
  const environments = useEnvironmentsWithHealth();
  const navigate = useNavigate();

  const { register, handleSubmit, formState, watch } = useForm<FormState>({
    mode: "onTouched",
  });

  const [
    roleDescription,
    materializeUseCase,
    projectDescription,
    primaryDataSource,
  ] = watch([
    "roleDescription",
    "materializeUseCase",
    "projectDescription",
    "primaryDataSource",
  ]);

  const { errors } = formState;

  const someEnvironmentNotDisabled = Array.from(environments.values()).some(
    (env) => env.state !== "disabled",
  );

  if (someEnvironmentNotDisabled) {
    return <Navigate to="../enable-region" />;
  }

  const onSubmit = handleSubmit(async (data) => {
    try {
      await submitOnboardingSurvey({
        ...data,
        email: user.email,
        organizationId: user.tenantId,
        userId: user.id,
      });
    } catch {
      // Even if this survey fails to submit, we don't want to block
      // onboarding
      console.error("Failed to submit onboarding survey");
    }
    navigate("../enable-region");
  });

  return (
    <chakra.form
      onSubmit={onSubmit}
      w="100%"
      h="100%"
      display="flex"
      flexDirection="column"
      data-testid="onboarding-survey"
    >
      <VStack w="100%" h="100%" py="10" px="16" spacing="0">
        <VStack w="100%" overflowY="auto" flex="1" minH={0}>
          <VStack
            w="100%"
            alignItems="flex-start"
            spacing="0"
            maxW="844px"
            pb="20"
          >
            <Text textStyle="heading-xl" mt={NAVBAR_HEIGHT}>
              Your information
            </Text>
            <Text textStyle="text-base" my="5">
              We&apos;d love to help you get started on the right foot with
              Materialize!
            </Text>
            <VStack spacing="10" width="100%">
              <FormControl isRequired isInvalid={!!errors.roleDescription}>
                <FormLabel>What is your role?</FormLabel>
                <SimpleSelect
                  {...register("roleDescription", { required: "Required*" })}
                  placeholder={PLACEHOLDER_TEXT}
                  autoFocus
                >
                  {ROLE_DESCRIPTION_OPTIONS.map((role) => (
                    <option key={role.value} value={role.value}>
                      {role.label}
                    </option>
                  ))}
                </SimpleSelect>
                {errors.roleDescription && (
                  <FormErrorMessage>
                    {errors.roleDescription.message}
                  </FormErrorMessage>
                )}
              </FormControl>
              {roleDescription === OTHER_OPTION.value && (
                <FormControl>
                  <FormLabel>Please describe your role</FormLabel>
                  <Textarea
                    size="sm"
                    autoFocus
                    {...register("roleDescriptionDetails")}
                  />
                </FormControl>
              )}

              <FormControl isRequired isInvalid={!!errors.materializeUseCase}>
                <FormLabel>What brings you to Materialize?</FormLabel>
                <SimpleSelect
                  {...register("materializeUseCase", { required: "Required*" })}
                  placeholder={PLACEHOLDER_TEXT}
                >
                  {MATERIALIZE_USE_CASE_OPTIONS.map((useCase) => (
                    <option key={useCase.value} value={useCase.value}>
                      {useCase.label}
                    </option>
                  ))}
                </SimpleSelect>
                {errors.materializeUseCase && (
                  <FormErrorMessage>
                    {errors.materializeUseCase.message}
                  </FormErrorMessage>
                )}
              </FormControl>
              {materializeUseCase === OTHER_OPTION.value && (
                <FormControl>
                  <FormLabel>Please provide more details</FormLabel>
                  <Textarea
                    size="sm"
                    autoFocus
                    {...register("materializeUseCaseDetails")}
                  />
                </FormControl>
              )}

              <FormControl isRequired isInvalid={!!errors.projectDescription}>
                <FormLabel>
                  What problem are you trying to solve with Materialize?
                </FormLabel>
                <SimpleSelect
                  {...register("projectDescription", { required: "Required*" })}
                  placeholder={PLACEHOLDER_TEXT}
                >
                  {PROJECT_DESCRIPTION_OPTIONS.map((project) => (
                    <option key={project.value} value={project.value}>
                      {project.label}
                    </option>
                  ))}
                </SimpleSelect>
                {errors.projectDescription && (
                  <FormErrorMessage>
                    {errors.projectDescription.message}
                  </FormErrorMessage>
                )}
              </FormControl>
              {projectDescription === OTHER_OPTION.value && (
                <FormControl>
                  <FormLabel>Please describe your problem</FormLabel>
                  <Textarea
                    size="sm"
                    autoFocus
                    {...register("projectDescriptionDetails")}
                  />
                </FormControl>
              )}

              <FormControl isRequired isInvalid={!!errors.primaryDataSource}>
                <FormLabel>What is your primary data source?</FormLabel>
                <SimpleSelect
                  {...register("primaryDataSource", { required: "Required*" })}
                  placeholder={PLACEHOLDER_TEXT}
                >
                  {PRIMARY_DATA_SOURCE_OPTIONS.map((source) => (
                    <option key={source.value} value={source.value}>
                      {source.label}
                    </option>
                  ))}
                </SimpleSelect>
                {errors.primaryDataSource && (
                  <FormErrorMessage>
                    {errors.primaryDataSource.message}
                  </FormErrorMessage>
                )}
              </FormControl>
              {primaryDataSource === OTHER_OPTION.value && (
                <FormControl>
                  <FormLabel>
                    Please describe your primary data source
                  </FormLabel>
                  <Textarea
                    size="sm"
                    autoFocus
                    {...register("primaryDataSourceDetails")}
                  />
                </FormControl>
              )}
            </VStack>
          </VStack>
        </VStack>
        <HStack
          marginBottom="10"
          flexShrink="0"
          marginTop="5"
          maxW="844px"
          width="100%"
          justifyContent="space-between"
          alignItems="flex-end"
          position="sticky"
          bottom="0"
          bg="background.primary"
          zIndex="1"
          py="2"
        >
          <ContactSalesCta />
          <HStack>
            <AppConfigSwitch
              cloudConfigElement={({ appConfig: { currentStack } }) =>
                // We only show the skip survey button for internal / non-production environments.
                currentStack !== "production" && (
                  <SkipSurveyButton
                    onClick={() => navigate("../enable-region")}
                  />
                )
              }
            />

            <Tooltip
              label="Answer the questions to proceed"
              fontSize="xs"
              isDisabled={formState.isValid}
            >
              <Button
                variant="primary"
                type="submit"
                isDisabled={!formState.isValid}
              >
                Choose region
              </Button>
            </Tooltip>
          </HStack>
        </HStack>
      </VStack>
    </chakra.form>
  );
};

export default OnboardingSurvey;
