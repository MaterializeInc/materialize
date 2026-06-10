// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export type RoleGrantError = {
  description: string;
  error: { errorMessage: string };
};

export type RoleGrantSuccess = {
  description: string;
};

export function getErrorMessage(reason: unknown): string {
  return reason instanceof Error ? reason.message : "Unknown error";
}

export function processResults<T>(
  results: PromiseSettledResult<unknown>[],
  items: T[],
  getDescription: (item: T) => string,
): {
  succeeded: RoleGrantSuccess[];
  failed: RoleGrantError[];
} {
  const succeeded: RoleGrantSuccess[] = [];
  const failed: RoleGrantError[] = [];

  results.forEach((result, i) => {
    const description = getDescription(items[i]);

    if (result.status === "fulfilled") {
      succeeded.push({ description });
    } else {
      failed.push({
        description,
        error: { errorMessage: getErrorMessage(result.reason) },
      });
    }
  });

  return { succeeded, failed };
}
