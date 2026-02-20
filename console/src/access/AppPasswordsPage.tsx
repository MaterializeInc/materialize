// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Alert as ChakraAlert,
  AlertDescription,
  BoxProps,
  Button,
  CloseButton,
  FormControl,
  FormErrorMessage,
  FormHelperText,
  FormLabel,
  HStack,
  Input,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Radio,
  RadioGroup,
  Stack,
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tooltip,
  Tr,
  useDisclosure,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import React, { useState } from "react";
import { Controller, useForm } from "react-hook-form";
import { useLocation } from "react-router-dom";

import DeleteAppPasswordModal from "~/access/DeleteAppPasswordModal";
import { hasTenantApiTokenPermissions } from "~/api/auth";
import { ApiToken } from "~/api/frontegg/types";
import Alert from "~/components/Alert";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import ConnectModal from "~/components/ConnectModal";
import { SecretCopyableBox } from "~/components/copyableComponents";
import TaggedMultiSelect from "~/components/Dropdown/TaggedComboBox";
import { LoadingContainer } from "~/components/LoadingContainer";
import { Modal } from "~/components/Modal";
import { User } from "~/external-library-wrappers/frontegg";
import {
  MainContentContainer,
  PageHeader,
  PageHeading,
} from "~/layouts/BaseLayout";
import {
  useCreateApiToken,
  useListApiTokens,
  useTeamRoles,
} from "~/queries/frontegg";
import ConnectionIcon from "~/svg/ConnectionIcon";
import { MaterializeTheme } from "~/theme";
import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

const AppPasswordsPage = ({ user }: { user: User }) => {
  const { isOpen, onOpen, onClose } = useDisclosure();
  const location = useLocation();

  React.useEffect(() => {
    if (location.state && "new" in location.state && location.state.new) {
      onOpen();
    }
  }, [location.pathname, location.state, onOpen]);

  return (
    <MainContentContainer>
      <PageHeader>
        <PageHeading>App Passwords</PageHeading>
      </PageHeader>
      <React.Suspense fallback={<LoadingContainer />}>
        <AppErrorBoundary>
          <AppPasswordsInner
            isNewModalOpen={isOpen}
            closeNewModal={onClose}
            user={user}
          />
        </AppErrorBoundary>
      </React.Suspense>
    </MainContentContainer>
  );
};
const AppPasswordsInner = (props: {
  isNewModalOpen: boolean;
  closeNewModal: () => void;
  user: User;
}) => {
  const { user } = props;

  const { data: roles } = useTeamRoles();

  const canAccessTenantApiTokens = hasTenantApiTokenPermissions(user);

  const {
    mutate: createAppPassword,
    isPending: createInProgress,
    data: newPassword,
    error,
  } = useCreateApiToken();

  const { data: appPasswords } = useListApiTokens({ user });

  const {
    register,
    handleSubmit,
    formState,
    reset,
    getValues,
    watch,
    control,
  } = useForm<{
    type: "personal" | "service";
    user: string;
    name: string;
    roles: { name: string; id: string }[];
  }>({
    mode: "onChange",
    defaultValues: {
      type: "personal",
      name: "",
      user: "",
      roles: [],
    },
  });

  const [newPasswordClosed, setNewPasswordClosed] = useState("");

  const watchType = watch("type");

  const isSecretBoxOpen =
    newPassword &&
    newPasswordClosed !== newPassword.clientId &&
    appPasswords.map((p) => p.clientId).includes(newPassword.clientId);

  return (
    <VStack alignItems="stretch">
      {error && <Alert variant="error" message={error.message} mb="10" />}
      {isSecretBoxOpen && (
        <SecretBox
          name={newPassword.description}
          password={newPassword.password}
          obfuscatedContent={newPassword.obfuscatedPassword}
          onClose={() => setNewPasswordClosed(newPassword.clientId)}
        />
      )}
      <Text fontSize="sm" mb="2">
        App passwords allow applications and services to connect to Materialize.
      </Text>
      <ApiTokensTableProps tokens={appPasswords} user={user} />
      <Modal
        isOpen={props.isNewModalOpen}
        onClose={props.closeNewModal}
        size="lg"
      >
        <ModalOverlay />
        <ModalContent>
          <form
            onSubmit={handleSubmit((data) => {
              createAppPassword({
                type: data.type,
                description: data.name,
                user: data.user,
                roleIds: data.roles.map((r) => r.id),
              });
              reset();
              props.closeNewModal();
            })}
          >
            <ModalHeader>New app password</ModalHeader>
            <ModalCloseButton />
            <ModalBody>
              <VStack pb={6} spacing="4">
                <FormControl>
                  <FormLabel htmlFor="name" fontSize="sm">
                    Type
                  </FormLabel>
                  <RadioGroup defaultValue={getValues("type")}>
                    <Stack spacing="-1">
                      <Radio {...register("type")} value="personal" size="sm">
                        Personal
                      </Radio>
                      <Radio
                        {...register("type")}
                        isDisabled={!canAccessTenantApiTokens}
                        value="service"
                        size="sm"
                      >
                        <Tooltip
                          label="Only Organization Admins can create service app passwords."
                          isDisabled={canAccessTenantApiTokens}
                          hasArrow
                          placement="right"
                          lineHeight="1.15"
                        >
                          Service
                        </Tooltip>
                      </Radio>
                    </Stack>
                  </RadioGroup>
                  <FormHelperText>
                    {watchType == "personal"
                      ? `Personal app passwords are associated with your user account (${user.email}).`
                      : "Service app passwords are associated with a user account of your choosing."}
                  </FormHelperText>
                </FormControl>
                <FormControl isInvalid={!!formState.errors.name}>
                  <FormLabel htmlFor="description" fontSize="sm">
                    Name
                  </FormLabel>
                  <Input
                    {...register("name", {
                      required: "Name is required",
                    })}
                    aria-label="Name"
                    placeholder="e.g. Personal laptop"
                    autoFocus={props.isNewModalOpen}
                    autoCorrect="off"
                    autoComplete="off"
                    size="sm"
                  />
                  <FormErrorMessage>
                    {formState.errors.name?.message}
                  </FormErrorMessage>
                  <FormHelperText>
                    Describe what you&apos;ll use the app password for, in case
                    you need to revoke it in the future.
                  </FormHelperText>
                </FormControl>
                {watchType == "service" && (
                  <>
                    <FormControl isInvalid={!!formState.errors.user}>
                      <FormLabel htmlFor="user" fontSize="sm">
                        User
                      </FormLabel>
                      <Input
                        {...register("user", {
                          validate: (value, formValues) => {
                            if (formValues.type === "service" && value === "") {
                              return "User is required.";
                            }
                            if (value.indexOf("@") !== -1) {
                              return "User cannot contain @ symbol.";
                            }
                            for (const prefix of ["mz_", "pg_", "external_"]) {
                              if (value.startsWith(prefix)) {
                                return `User cannot start with "${prefix}".`;
                              }
                            }
                          },
                        })}
                        autoCorrect="off"
                        autoComplete="off"
                        size="sm"
                      />
                      <FormHelperText>
                        If the specified user does not already exist, it will be
                        automatically created the first time the app password is
                        used.
                      </FormHelperText>
                      <FormErrorMessage>
                        {formState.errors.user?.message}
                      </FormErrorMessage>
                    </FormControl>
                    <FormControl isInvalid={!!formState.errors.roles}>
                      <FormLabel htmlFor="roles" fontSize="sm">
                        Roles
                      </FormLabel>
                      <Controller
                        name="roles"
                        control={control}
                        rules={{
                          validate: (value, formValues) => {
                            if (
                              formValues.type === "service" &&
                              value.length === 0
                            ) {
                              return "At least one role is required.";
                            }
                          },
                        }}
                        render={({ field }) => {
                          return (
                            <TaggedMultiSelect
                              items={roles}
                              selectedItems={field.value}
                              getItemLabel={(r) => r.name}
                              onChange={field.onChange}
                              placeholder="Select..."
                            />
                          );
                        }}
                      ></Controller>
                      <FormHelperText>
                        The roles to associate with the app password.
                      </FormHelperText>
                      <FormErrorMessage>
                        {formState.errors.roles?.message}
                      </FormErrorMessage>
                    </FormControl>
                  </>
                )}
              </VStack>
            </ModalBody>

            <ModalFooter>
              <HStack spacing="2">
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={props.closeNewModal}
                >
                  Cancel
                </Button>
                <Button
                  type="submit"
                  variant="primary"
                  size="sm"
                  isDisabled={!!createInProgress}
                >
                  Create Password
                </Button>
              </HStack>
            </ModalFooter>
          </form>
        </ModalContent>
      </Modal>
    </VStack>
  );
};

type ApiTokensTableProps = BoxProps & {
  tokens: ApiToken[];
  user: User;
};

const ApiTokensTableProps = ({
  tokens,
  user,
  ...props
}: ApiTokensTableProps) => {
  const { roles } = user;
  const { colors } = useTheme<MaterializeTheme>();

  const rolesByKey = new Map(roles.map((r) => [r.key, r.name]));
  const rolesById = new Map(roles.map((r) => [r.id, r.name]));

  return (
    <Table variant="standalone" {...props}>
      <Thead>
        <Tr>
          <Th>Name</Th>
          <Th>Type</Th>
          <Th>User</Th>
          <Th>Roles</Th>
          <Th>Created at</Th>
          <Th />
        </Tr>
      </Thead>
      <Tbody>
        {tokens.map((token) => {
          const userStr = token.type === "personal" ? user.email : token.user;

          let tokenRoles;
          if (token.type === "personal") {
            tokenRoles = user.roles.map((r) => rolesByKey.get(r.key)!);
          } else {
            tokenRoles = token.roleIds.map((r) => rolesById.get(r)!);
          }

          return (
            <Tr
              key={token.clientId}
              textColor="default"
              aria-label={token.description}
            >
              <Td
                borderBottomWidth="1px"
                borderBottomColor={colors.border.primary}
              >
                {token.description}
              </Td>
              <Td
                borderBottomWidth="1px"
                borderBottomColor={colors.border.primary}
              >
                {token.type === "personal" ? "Personal" : "Service"}
              </Td>
              <Td
                borderBottomWidth="1px"
                borderBottomColor={colors.border.primary}
              >
                {token.type === "personal" ? (
                  <Text color={colors.gray["500"]}>{userStr}</Text>
                ) : (
                  userStr
                )}
              </Td>
              <Td
                borderBottomWidth="1px"
                borderBottomColor={colors.border.primary}
              >
                {token.type === "personal" ? (
                  <Text color={colors.gray["500"]}>
                    {tokenRoles.join(", ")}
                  </Text>
                ) : (
                  tokenRoles.join(", ")
                )}
              </Td>
              <Td
                borderBottomWidth="1px"
                borderBottomColor={colors.border.primary}
              >
                {" "}
                {formatDate(
                  new Date(token.createdAt),
                  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
                )}
              </Td>
              <Td
                borderBottomWidth="1px"
                borderBottomColor={colors.border.primary}
              >
                <HStack>
                  <ConnectAppPasswordModal user={user} userStr={userStr} />
                  <DeleteAppPasswordModal token={token} />
                </HStack>
              </Td>
            </Tr>
          );
        })}
        {tokens.length === 0 && (
          <Tr>
            <Td colSpan={6}>No app passwords yet.</Td>
          </Tr>
        )}
      </Tbody>
    </Table>
  );
};

type SecretBoxProps = {
  name: string;
  password: string;
  obfuscatedContent: string;
  onClose: () => void;
};

const SecretBox = ({
  name,
  password,
  obfuscatedContent,
  onClose,
}: SecretBoxProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <ChakraAlert
      status="info"
      mb={2}
      size="sm"
      background={colors.background.info}
      borderRadius="md"
      borderWidth="1px"
      borderColor={colors.border.info}
    >
      <VStack alignItems="flex-start" width="100%">
        <AlertDescription width="100%" px={2}>
          <VStack alignItems="start">
            <Text fontSize="md" fontWeight="500">
              New password {`"${name}"`}:
            </Text>
            <SecretCopyableBox
              label="clientId"
              contents={password}
              obfuscatedContent={obfuscatedContent}
            />
          </VStack>
          <Text pt={1} textStyle="text-base" color={colors.foreground.primary}>
            Write this down; you will not be able to see your app password again
            after you reload!
          </Text>
        </AlertDescription>
      </VStack>
      <CloseButton
        position="absolute"
        right={1}
        top={1}
        size="sm"
        color={colors.foreground.secondary}
        onClick={onClose}
      />
    </ChakraAlert>
  );
};

const ConnectAppPasswordModal = ({
  user,
  userStr,
}: {
  user: User;
  userStr: string;
}) => {
  const { isOpen, onOpen, onClose } = useDisclosure();

  return (
    <>
      <Button
        onClick={onOpen}
        title="Connect to Materialize"
        size="sm"
        colorScheme="primary"
        variant="outline"
        leftIcon={<ConnectionIcon />}
      >
        Connect
      </Button>
      <ConnectModal
        onClose={onClose}
        isOpen={isOpen}
        user={user}
        forAppPassword={{ user: userStr }}
      />
    </>
  );
};

export default AppPasswordsPage;
