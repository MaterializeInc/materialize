// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { exec } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

import dedent from "dedent";

const DIRNAME = path.dirname(fileURLToPath(import.meta.url));
const TESTDRIVE_PATH = path.resolve(DIRNAME, "../../../../test/console");
const COMPOSE_PATH = path.resolve(TESTDRIVE_PATH, "mzcompose");

export async function getMaterializeEndpoint(port: number) {
  const { stdout } = await mzcompose(`port materialized ${port}`);
  const [host, mappedPort] = stdout.split(":");

  return { host, port: parseInt(mappedPort) };
}

export async function getMaterializeEndpoints() {
  // When running inside the Docker composition (via mzcompose), endpoints are
  // passed as environment variables so we can reach services by hostname.
  if (process.env.MZ_INTERNAL_SQL_HOST && process.env.MZ_INTERNAL_HTTP_HOST) {
    return {
      internalSql: {
        host: process.env.MZ_INTERNAL_SQL_HOST,
        port: parseInt(process.env.MZ_INTERNAL_SQL_PORT ?? "6877"),
      },
      internalHttp: {
        host: process.env.MZ_INTERNAL_HTTP_HOST,
        port: parseInt(process.env.MZ_INTERNAL_HTTP_PORT ?? "6878"),
      },
    };
  }

  // Fallback: resolve host-mapped ports via mzcompose (for local development).
  const internalSql = await getMaterializeEndpoint(6877);
  const internalHttp = await getMaterializeEndpoint(6878);

  return { internalSql, internalHttp };
}

export const dedenterWithEscapeSpecialCharacters = dedent.withOptions({
  escapeSpecialCharacters: true,
});
export const dedenter = dedent.withOptions({ escapeSpecialCharacters: false });

/**
 * Executes testdrive with the given test script.
 */
export async function testdrive(
  input: string,
  options: {
    noReset?: boolean;
    escapeSpecialCharacters?: boolean;
    timeoutSeconds?: number;
  } = {
    // Template strings automatically escape certain characters by default, but we disable
    // that behavior.
    escapeSpecialCharacters: true,
    timeoutSeconds: 1,
  },
) {
  return docker(
    "exec",
    [
      "-i",
      "console-testdrive-1",
      "testdrive",
      "--kafka-addr=kafka:9092",
      "--schema-registry-url=http://schema-registry:8081",
      "--materialize-url=postgres://materialize@materialized:6875",
      "--materialize-internal-url=postgres://materialize@materialized:6877",
      options.noReset ? "--no-reset" : "",
      options.timeoutSeconds
        ? `--default-timeout ${options.timeoutSeconds}s`
        : "",
    ],
    options.escapeSpecialCharacters
      ? dedenterWithEscapeSpecialCharacters(input)
      : dedenter(input),
  );
}

export async function docker(
  command: string,
  options: string[] = [],
  input?: string,
) {
  return new Promise<{ stdout: string; stderr: string }>((resolve, reject) => {
    const childProccess = exec(
      `docker ${command} ${options.join(" ")}`,
      (error, stdout, stderr) => {
        if (error) {
          reject({ error, stdout, stderr });
          return;
        }
        resolve({ stdout, stderr });
      },
    );
    if (input) {
      childProccess.stdin?.write(input);
      childProccess.stdin?.end();
    }
  });
}

export async function mzcompose(command: string, input?: string) {
  return new Promise<{ stdout: string; stderr: string }>((resolve, reject) => {
    const childProccess = exec(
      `${COMPOSE_PATH} ${command}`,
      { cwd: TESTDRIVE_PATH },
      (error, stdout, stderr) => {
        if (error) {
          reject(error);
          return;
        }
        resolve({ stdout, stderr });
      },
    );
    if (input) {
      childProccess.stdin?.write(input);
      childProccess.stdin?.end();
    }
  });
}
