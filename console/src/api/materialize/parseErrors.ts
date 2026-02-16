// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Looks for "object already exists" error messages and returns the object name
 * @returns string object name or null
 */
export function alreadyExistsError(errorMessage?: string) {
  if (!errorMessage) {
    return null;
  }

  /**
   * This regex takes a string and extracts a substring in single or double quotation marks
   * only if the sentence ends with "already exists".
   */
  const strInsideQuotesMatch = /['"]([^'"]*)['"][\s\S]*already exists$/g.exec(
    errorMessage,
  );

  if (strInsideQuotesMatch && strInsideQuotesMatch.length > 1) {
    return strInsideQuotesMatch[1];
  }

  return null;
}

/**
 * Looks for "cannot create multiple replicas named" error messages and returns the object name
 * @returns string object name or null
 */
export function duplicateReplicaName(errorMessage?: string) {
  if (!errorMessage) {
    return null;
  }

  /**
   * This regex takes a string and extracts a substring in single or double quotation marks
   * only if the sentence ends with "already exists".
   */
  const strInsideQuotesMatch =
    /cannot create multiple replicas named ['"]([^'"]*)['"][\s\S]*on cluster '.*'$/g.exec(
      errorMessage,
    );

  if (strInsideQuotesMatch && strInsideQuotesMatch.length > 1) {
    return strInsideQuotesMatch[1];
  }

  return null;
}
