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
  IconButton,
  Input,
  InputGroup,
  InputRightElement,
  Link,
  Spinner,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import { useMutation } from "@tanstack/react-query";
import React, { useState } from "react";
import { useForm } from "react-hook-form";
import { useNavigate, useSearchParams } from "react-router-dom";

import { LOGIN_ERROR_PARAM, loginOrThrow } from "~/api/materialize/auth";
import Alert from "~/components/Alert";
import { LabeledInput } from "~/components/formComponentsV2";
import { MaterializeLogo } from "~/components/MaterializeLogo";
import { useAppConfig } from "~/config/useAppConfig";
import { useAuth } from "~/external-library-wrappers/oidc";
import { AuthContentContainer, AuthLayout } from "~/layouts/AuthLayout";
import EyeClosedIcon from "~/svg/EyeClosedIcon";
import EyeOpenIcon from "~/svg/EyeOpenIcon";
import { MaterializeTheme } from "~/theme";

type LoginFormState = {
  username: string;

  password: string;
};

const PasswordLoginForm = () => {
  const { formState, handleSubmit, register, setError } =
    useForm<LoginFormState>({
      defaultValues: {
        username: "",
        password: "",
      },
      mode: "onTouched",
    });

  const [generalFormError, setGeneralFormError] = useState<string | null>(null);
  const [showPassword, setShowPassword] = useState(false);

  const { mutate: login } = useMutation({
    mutationFn: loginOrThrow,
  });

  const navigate = useNavigate();

  const handleValidSubmit = (values: LoginFormState) => {
    login(
      { payload: values },
      {
        onSuccess: () => {
          const redirectUrl = "/";
          navigate(redirectUrl, { replace: true });
        },
        onError: (loginError) => {
          if (loginError instanceof Error) {
            setError("username", {});
            setError("password", {});

            setGeneralFormError(loginError.message);
          }
        },
      },
    );
  };

  return (
    <form onSubmit={handleSubmit(handleValidSubmit)}>
      <VStack spacing="6" alignItems="start">
        {generalFormError && (
          <Alert variant="error" minWidth="100%" message={generalFormError} />
        )}
        <FormControl isInvalid={!!formState.errors.username}>
          <LabeledInput
            label="Username"
            error={formState.errors.username?.message}
            variant="stretch"
          >
            <Input
              {...register("username", {
                required: "Username is required.",
              })}
              autoCorrect="off"
              placeholder="Enter your username"
              size="lg"
              variant={formState.errors.username ? "error" : "default"}
            />
          </LabeledInput>
        </FormControl>

        <FormControl isInvalid={!!formState.errors.password}>
          <LabeledInput
            label="Password"
            error={formState.errors.password?.message}
            variant="stretch"
          >
            <InputGroup>
              <Input
                {...register("password", {
                  required: "Password is required.",
                })}
                autoCorrect="off"
                placeholder="Enter your password"
                size="lg"
                type={showPassword ? "text" : "password"}
                variant={formState.errors.password ? "error" : "default"}
              />
              <InputRightElement h="full">
                <IconButton
                  variant="unstyled"
                  aria-label={showPassword ? "Hide password" : "Show password"}
                  icon={showPassword ? <EyeOpenIcon /> : <EyeClosedIcon />}
                  onClick={() => setShowPassword(!showPassword)}
                  size="lg"
                />
              </InputRightElement>
            </InputGroup>
          </LabeledInput>
        </FormControl>

        <Button
          variant="primary"
          size="lg"
          type="submit"
          isLoading={false}
          spinner={<Spinner />}
          width="100%"
        >
          Sign in
        </Button>
      </VStack>
    </form>
  );
};

const SsoLoginLink = () => {
  const { colors } = useTheme<MaterializeTheme>();
  const auth = useAuth();
  const [error, setError] = useState<string | null>(null);

  if (!auth) return null;

  const handleSsoLogin = () => {
    setError(null);
    auth.signinRedirect().catch((err: unknown) => {
      setError(
        err instanceof Error ? err.message : "Failed to initiate SSO login",
      );
    });
  };

  return (
    <VStack spacing="2" alignItems="center">
      {error && <Alert variant="error" minWidth="100%" message={error} />}
      <Link
        color={colors.accent.brightPurple}
        fontSize="sm"
        onClick={handleSsoLogin}
        cursor="pointer"
        textDecoration="none"
        _hover={{ textDecoration: "underline" }}
      >
        Use single sign-on
      </Link>
    </VStack>
  );
};

export const Login = () => {
  const appConfig = useAppConfig();
  const [searchParams] = useSearchParams();
  const isOidc =
    appConfig.mode === "self-managed" && appConfig.authMode === "Oidc";

  const errorMessage = searchParams.get(LOGIN_ERROR_PARAM);

  return (
    <AuthLayout>
      <AuthContentContainer>
        <VStack alignItems="stretch" width="100%" mx="12">
          <HStack my={{ base: "8", lg: "0" }} paddingBottom="8">
            <MaterializeLogo height="12" />
          </HStack>
          {errorMessage && (
            <Alert
              variant="error"
              minWidth="100%"
              message={errorMessage}
              mb="4"
            />
          )}
          <PasswordLoginForm />
          {isOidc && <SsoLoginLink />}
        </VStack>
      </AuthContentContainer>
    </AuthLayout>
  );
};
