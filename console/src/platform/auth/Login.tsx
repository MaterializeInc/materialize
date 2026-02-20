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
  Spinner,
  VStack,
} from "@chakra-ui/react";
import { useMutation } from "@tanstack/react-query";
import React, { useState } from "react";
import { useForm } from "react-hook-form";
import { useNavigate } from "react-router-dom";

import { loginOrThrow } from "~/api/materialize/auth";
import Alert from "~/components/Alert";
import { LabeledInput } from "~/components/formComponentsV2";
import { MaterializeLogo } from "~/components/MaterializeLogo";
import { AuthContentContainer, AuthLayout } from "~/layouts/AuthLayout";
import EyeClosedIcon from "~/svg/EyeClosedIcon";
import EyeOpenIcon from "~/svg/EyeOpenIcon";

type LoginFormState = {
  username: string;

  password: string;
};

export const Login = () => {
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
    <AuthLayout>
      <AuthContentContainer>
        <VStack alignItems="stretch" width="100%" mx="12">
          <HStack my={{ base: "8", lg: "0" }} paddingBottom="8">
            <MaterializeLogo height="12" />
          </HStack>
          <form onSubmit={handleSubmit(handleValidSubmit)}>
            <VStack spacing="6" alignItems="start">
              {generalFormError && (
                <Alert
                  variant="error"
                  minWidth="100%"
                  message={generalFormError}
                />
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
                        aria-label={
                          showPassword ? "Hide password" : "Show password"
                        }
                        icon={
                          showPassword ? <EyeOpenIcon /> : <EyeClosedIcon />
                        }
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
        </VStack>
      </AuthContentContainer>
    </AuthLayout>
  );
};
