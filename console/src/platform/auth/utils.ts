// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ValidateResult } from "react-hook-form";

export function validatePassword(): Record<
  string,
  (value: string) => ValidateResult
> {
  return {
    min: (value: string) => value.length >= 10,
    uppercase: (value: string) => Boolean(value.match(/[A-Z]/)),
    lowercase: (value: string) => Boolean(value.match(/[a-z]/)),
    number: (value: string) => Boolean(value.match(/\d/)),
    special: (value: string) => Boolean(value.match(/\W/)),
    repeating: (value: string) => !value.match(/(\w)\1\1/),
  };
}

export const passwordRules: Record<
  keyof ReturnType<typeof validatePassword>,
  string
> = {
  min: "be at least 10 characters",
  uppercase: "contain at least one uppercase character",
  lowercase: "contain at least one lowercase character",
  number: "contain at least one number",
  special: "contain at least one special character",
  repeating: "not contain 3 or more repeating characters",
};
