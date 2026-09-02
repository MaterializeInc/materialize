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
import React, { useEffect, useState } from "react";
import { useForm } from "react-hook-form";
import { useNavigate, useSearchParams } from "react-router-dom";

import { LOGIN_ERROR_PARAM, loginOrThrow } from "~/api/materialize/auth";
import Alert from "~/components/Alert";
import { LabeledInput } from "~/components/formComponentsV2";
import { MaterializeLogo } from "~/components/MaterializeLogo";
import { useAuth, useOidcManagerQuery } from "~/external-library-wrappers/oidc";
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

const SsoSignInButton = ({ onClick }: { onClick: () => void }) => (
  <Button variant="primary" size="lg" width="100%" onClick={onClick}>
    Sign in with SSO
  </Button>
);

const AuthModeToggleLink = ({
  label,
  onClick,
}: {
  label: string;
  onClick: () => void;
}) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Link
      alignSelf="center"
      color={colors.accent.brightPurple}
      fontSize="sm"
      onClick={onClick}
      cursor="pointer"
      textDecoration="none"
      _hover={{ textDecoration: "underline" }}
    >
      {label}
    </Link>
  );
};

export const Login = () => {
  const { data: oidcManager, error: oidcInitializationError } =
    useOidcManagerQuery();
  const auth = useAuth();
  const [searchParams, setSearchParams] = useSearchParams();

  // Surface the one-shot auth error from the redirect, then strip it from the
  // URL so it doesn't reappear on refresh or back navigation.
  const [oidcError, setOidcError] = useState<string | null>(() =>
    searchParams.get(LOGIN_ERROR_PARAM),
  );

  useEffect(() => {
    if (!searchParams.has(LOGIN_ERROR_PARAM)) return;
    const nextParams = new URLSearchParams(searchParams);
    nextParams.delete(LOGIN_ERROR_PARAM);
    setSearchParams(nextParams, { replace: true });
  }, [searchParams, setSearchParams]);

  const isOidcAvailable = Boolean(oidcManager && auth);
  const [showPasswordForm, setShowPasswordForm] = useState(false);
  const [ssoError, setSsoError] = useState<string | null>(null);

  // `react-oidc-context` swallows some redirect failures into `auth.error`
  // instead of rejecting the promise, so surface that too.
  const ssoAuthError = auth?.error?.message ?? null;
  const ssoDisplayError =
    ssoError ||
    (ssoAuthError &&
      `${ssoAuthError}. It looks like there may be an issue with the sign-in configuration. Please review your OIDC settings or check the console logs for more information.`) ||
    null;

  const handleSsoLogin = () => {
    if (!auth) return;
    setSsoError(null);
    auth.signinRedirect().catch((err: unknown) => {
      setSsoError(
        err instanceof Error ? err.message : "Failed to initiate SSO login",
      );
    });
  };

  return (
    <AuthLayout>
      <AuthContentContainer>
        <VStack alignItems="stretch" width="100%" mx="12">
          <HStack my={{ base: "8", lg: "0" }} paddingBottom="8">
            <MaterializeLogo height="12" />
          </HStack>
          {oidcError && (
            <Alert
              variant="error"
              minWidth="100%"
              message={oidcError}
              mb="4"
              onClose={() => setOidcError(null)}
            />
          )}
          {oidcInitializationError && (
            <Alert
              variant="info"
              minWidth="100%"
              message={oidcInitializationError.message}
              mb="4"
            />
          )}
          {isOidcAvailable && ssoDisplayError && (
            <Alert
              variant="error"
              minWidth="100%"
              message={ssoDisplayError}
              mb="4"
            />
          )}
          {isOidcAvailable ? (
            <VStack alignItems="stretch" spacing="4">
              {showPasswordForm ? (
                <>
                  <PasswordLoginForm />
                  <AuthModeToggleLink
                    label="Sign in with SSO"
                    onClick={handleSsoLogin}
                  />
                </>
              ) : (
                <>
                  <SsoSignInButton onClick={handleSsoLogin} />
                  <AuthModeToggleLink
                    label="Sign in with SQL password authentication"
                    onClick={() => setShowPasswordForm(true)}
                  />
                </>
              )}
            </VStack>
          ) : (
            <PasswordLoginForm />
          )}
        </VStack>
      </AuthContentContainer>
    </AuthLayout>
  );
};
