// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { FieldError } from "react-hook-form";

export const replicasErrorMessage = (
  error: FieldError | undefined,
  maxReplicas: number,
) => {
  if (!error?.type) return error?.message;
  if (error.type === "min") return "Replicas cannot be negative.";
  if (error.type === "max") return `Replicas cannot exceed ${maxReplicas}.`;
  if (error.type === "required") return "Replicas is required.";
};
