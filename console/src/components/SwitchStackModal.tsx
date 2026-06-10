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
  Button,
  FormControl,
  FormErrorMessage,
  FormLabel,
  HStack,
  IconButton,
  Image,
  Input,
  keyframes,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Radio,
  RadioGroup,
  Stack,
  Text,
  useDisclosure,
  useTheme,
} from "@chakra-ui/react";
import React, { useState } from "react";
import { useController, useForm } from "react-hook-form";

import { Modal } from "~/components/Modal";
import { getFronteggUrl } from "~/config/apiUrls";
import { getCurrentStack, setCurrentStack } from "~/config/currentStack";
import { useAppConfig } from "~/config/useAppConfig";
import useClickAndHold from "~/hooks/useClickAndHold";
import laserEyes from "~/img/laser-eyes.png";
import { MaterializeTheme } from "~/theme";

import { MaterializeLogo } from "./MaterializeLogo";

const getStackName = (data: {
  stackName: string;
  personalStackName: string;
}) => {
  if (data.stackName === "personal") {
    return data.personalStackName;
  }
  return data.stackName;
};

const isProductionDomain = () =>
  window.location.hostname === "cloud.materialize.com" ||
  window.location.hostname === "console.materialize.com";

const pulse = keyframes`
  0% { opacity: 0.7; }
  50% { opacity: 1; }
  100% { opacity: 0.7; }
`;

/**
 * A modal that allows switching which backend stack to use.
 */
const SwitchStackModalContent = () => {
  const {
    control,
    register,
    handleSubmit,
    formState,
    reset,
    setValue,
    setError,
  } = useForm<{
    stackName: string;
    personalStackName: string;
  }>({
    mode: "onTouched",
  });
  const { colors } = useTheme<MaterializeTheme>();
  const [easterEggEnabled, setEasterEggEnabled] = useState(false);
  // This is a work around from the Chakra RadioGroup onChange not providing an event parameter
  const { field: personalStackField } = useController({
    name: "stackName",
    control,
    rules: { required: "Please select a stack." },
  });
  const { isOpen, onOpen, onClose } = useDisclosure({
    onClose: () => {
      reset();
    },
  });
  const { startHold, endHold } = useClickAndHold(3000, () =>
    setEasterEggEnabled((enabled) => !enabled),
  );

  const isValidStack = async (stack: string) => {
    const baseUrl = getFronteggUrl(stack);
    try {
      const response = await fetch(
        baseUrl + "/frontegg/identity/resources/configurations/v1/public",
      );
      return response.ok;
    } catch (error) {
      return false;
    }
  };

  return (
    <>
      <IconButton
        aria-label="Switch stack"
        backgroundColor={colors.background.secondary}
        border="2px"
        borderColor={colors.border.secondary}
        borderRadius="full"
        color={colors.foreground.primary}
        display="flex"
        alignItems="center"
        justifyContent="center"
        fontWeight={500}
        icon={
          <>
            {easterEggEnabled && (
              <Image
                src={laserEyes}
                position="absolute"
                animation={`${pulse} 2s infinite`}
              />
            )}
            <MaterializeLogo markOnly aria-label="Switch stack" />
          </>
        }
        onClick={onOpen}
        px={2}
        py={2}
        size="sm"
        height="16"
        width="16"
        title="Switch stack"
        onMouseDown={startHold}
        onMouseUp={endHold}
        onMouseOut={endHold}
      />

      <Modal
        size="3xl"
        isOpen={isOpen}
        onClose={onClose}
        data-testid="switch-stack-modal"
      >
        <ModalOverlay />
        <form
          onSubmit={handleSubmit(async (data) => {
            const isPersonal = data.stackName === "personal";
            if (isPersonal && !data.personalStackName) {
              setError("personalStackName", {
                type: "custom",
                message: "Please enter a personal stack name.",
              });
              return;
            }
            const name = getStackName(data);
            const valid = await isValidStack(name);
            if (valid) {
              setCurrentStack(name);
              location.reload();
            } else {
              setError(isPersonal ? "personalStackName" : "stackName", {
                type: "custom",
                message: `${getFronteggUrl(
                  name,
                )} is not reachable from this origin.`,
              });
            }
          })}
        >
          <ModalContent>
            <ModalHeader fontWeight="500">Switch Stacks</ModalHeader>
            <ModalCloseButton />
            <ModalBody pt="2" pb="6" alignItems="stretch">
              <FormControl isInvalid={!!formState.errors.stackName}>
                <Text color="foreground.secondary" fontSize="sm" my="4">
                  Current Stack:{" "}
                  {getCurrentStack({
                    hostname: location.hostname,
                    isBrowser: true,
                  })}
                </Text>
                <FormLabel htmlFor="stackName" fontSize="sm">
                  Stack Name
                </FormLabel>
                <RadioGroup {...register("stackName")} {...personalStackField}>
                  <Stack direction="column">
                    {isProductionDomain() && (
                      <Radio value="production">Production</Radio>
                    )}
                    <Radio value="staging">Staging</Radio>
                    <Radio value="local">Local</Radio>
                    <Radio value="personal">Personal</Radio>
                  </Stack>
                </RadioGroup>
                <FormErrorMessage>
                  {formState.errors.stackName?.message}
                </FormErrorMessage>
              </FormControl>
              <FormControl isInvalid={!!formState.errors.personalStackName}>
                <Box ml="6" mt="2">
                  <Input
                    {...register("personalStackName")}
                    onFocus={() => setValue("stackName", "personal")}
                    placeholder="$USER.$ENV"
                    autoFocus={isOpen}
                    autoCorrect="off"
                    size="sm"
                  />
                  <FormErrorMessage>
                    {formState.errors.personalStackName?.message}
                  </FormErrorMessage>
                </Box>
              </FormControl>
            </ModalBody>

            <ModalFooter>
              <HStack spacing="2">
                <Button variant="secondary" size="sm" onClick={onClose}>
                  Cancel
                </Button>
                <Button type="submit" variant="primary" size="sm">
                  Switch
                </Button>
              </HStack>
            </ModalFooter>
          </ModalContent>
        </form>
      </Modal>
    </>
  );
};

const SwitchStackModal = () => {
  const appConfig = useAppConfig();

  if (appConfig.mode === "cloud" && appConfig.stackSwitcherEnabled) {
    return <SwitchStackModalContent />;
  }

  return null;
};

export default SwitchStackModal;
