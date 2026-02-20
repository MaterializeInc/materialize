// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * @module
 * Generic utility functions.
 */

import base32 from "base32-encoding";
// eslint-disable-next-line unicorn/prefer-node-protocol
import { Buffer } from "buffer";
import { fromZonedTime } from "date-fns-tz";
import { stringify } from "uuid";

import { IPostgresInterval } from "./api/materialize";
import { OpenApiFetchError } from "./api/OpenApiFetchError";

// used by base32-encoding
if (typeof window === "object") {
  window.Buffer = Buffer;
}

export function isMac() {
  return navigator.userAgent.includes("Mac");
}

export function controlOrCommand() {
  return isMac() ? "⌘" : "Ctrl";
}

/**
 * Asserts that the specified condition is truthy.

 * Modeled after the function of the same name in Node.js.
 */
export function assert(condition: any, message?: string): asserts condition {
  if (!condition) {
    throw new Error(message ?? "assertion failed");
  }
}

/**
 * Detects the presence of ?noPoll query string parameter, which we use to disable
 * polling for development purposes.
 */
export function isPollingDisabled() {
  const params = new URLSearchParams(location.search);
  return Array.from(params.keys()).includes("noPoll");
}

/**
 * Determines if the user is an internal Materialize employee.
 *
 * This should only be used for displaying debugging information. It is not a valid method of security.
 */
export function isMzInternalEmail(email: string | undefined): boolean {
  return (
    !!email &&
    (email.endsWith("@materialize.com") ||
      email.endsWith("@materialize.io") ||
      email.endsWith("@mtrlz.com") ||
      email.endsWith("@mtrlz.io"))
  );
}

/**
 * Returns true unless an object is null or undefined and narrows the type correctly.
 */
export function notNullOrUndefined<T>(value: T): value is NonNullable<T> {
  return value !== null && value !== undefined;
}

/**
 * Returns truthiness of any object and narrows the type correctly.
 */
export function isTruthy<T>(
  value?: T | undefined | null | false | "" | 0,
): value is T {
  return !!value;
}

/**
 * Returns singular if count is exactly 1, otherwise returns plural.
 */
export function pluralize(
  count: number | bigint,
  singular: string,
  plural: string,
) {
  if (count === 1) {
    return singular;
  }
  return plural;
}

/**
 * Capitalizes a string by capitalizing the first letter and optionally appending a period.
 */
export function capitalizeSentence(str: string, withPeriod = true) {
  let newStr = str.charAt(0).toUpperCase() + str.slice(1);
  if (withPeriod && str.charAt(str.length - 1) !== ".") {
    newStr += ".";
  }

  return newStr;
}

/**
 * e.g. "some-example-string" returns a "Some example string".
 */
export function kebabToSentenceCase(input: string): string {
  return input
    .split("-")
    .map((word, index) =>
      index === 0 ? capitalizeSentence(word, false) : word.toLowerCase(),
    )
    .join(" ");
}

/**
 * e.g. "some-example-string" returns a "Some Example String".
 */
export function kebabToTitleCase(input: string): string {
  return input
    .split("-")
    .map((word) => capitalizeSentence(word, false))
    .join(" ");
}

/**
 * e.g., "mysql" returns "MySQL"
 */
export function prettyConnectorType(type: string): string {
  switch (type) {
    case "mysql":
      return "MySQL";
    default:
      return kebabToTitleCase(type);
  }
}

/**
 * e.g. "some-example-string" returns a "SOME EXAMPLE STRING".
 */
export function kebabToScreamingSpaceCase(input: string): string {
  return input.split("-").join(" ").toUpperCase();
}

/**
 * e.g. "some_example_string" returns a "someExampleString".
 */
export function snakeToCamelCase(input: string): string {
  return input
    .split("_")
    .map((word, index) =>
      index === 0 ? word : capitalizeSentence(word, false),
    )
    .join("");
}

/**
 * e.g. "some_example_string" returns a "Some example string".
 */
export function snakeToSentenceCase(input: string): string {
  return input
    .split("_")
    .map((word, index) =>
      index === 0 ? capitalizeSentence(word, false) : word.toLowerCase(),
    )
    .join(" ");
}

/** Constrains a given number to a min and max. */
export function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(value, max));
}

/** Narrrows unknown error type to OpenApiFetchError */
export function isApiError(error: unknown): error is OpenApiFetchError {
  return typeof error === "object" && error !== null && "status" in error;
}

/** RFC 4648 Base32 alphabet */
export const BASE32_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/** Decodes an RFC 4648 encoded UUID */
export function base32UuidDecode(encoded: string) {
  const buffer = base32.parse(encoded, BASE32_CHARS);
  return stringify(buffer);
}

export function isSafari() {
  return /^((?!chrome|android).)*safari/i.test(navigator.userAgent);
}

export function maybeParseFloat(val: string | null | undefined) {
  if (!val) return null;
  return parseFloat(val);
}

export function nowUTC(): Date {
  return fromZonedTime(
    new Date(),
    Intl.DateTimeFormat().resolvedOptions().timeZone,
  );
}

export function exhaustiveGuard(val: never): never {
  throw new Error(`Unhandled switch case: ${val}`);
}

const byteExponentUnits: Array<[number, string]> = [
  [1, "KB"],
  [2, "MB"],
  [3, "GB"],
  [4, "TB"],
  [5, "PB"],
];

export function formatBytes(bytes: number): [number, string] {
  if (bytes < 1024) return [bytes, "B"];
  for (const [exponent, unit] of byteExponentUnits) {
    const value = bytes / Math.pow(1024, exponent);
    if (value < 1024) return [value, unit];
  }
  const [exponent, unit] = byteExponentUnits[byteExponentUnits.length - 1];
  return [bytes / Math.pow(1024, exponent), unit];
}

export function isInvalidDate(date: Date) {
  return isNaN(date.getTime());
}

/**
 * Converts milliseconds to a duration object to day precision.
 */
export function convertMillisecondsToDuration(ms: number) {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);
  return {
    days: days,
    hours: hours % 24,
    minutes: minutes % 60,
    seconds: seconds % 60,
    milliseconds: ms % 1000,
  };
}

/**
 * Escape a string so it can be included in a regex pattern.
 *
 * Source: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Regular_expressions#escaping
 */
export function escapeRegExp(literal: string) {
  return literal.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); // $& means the whole matched string
}

/**
 * Returns all values for given search param name, or a default.
 */
export function decodeArraySearchParam<T>(
  searchParams: URLSearchParams,
  name: string,
  defaultValue: T,
): string[] | T {
  const params = searchParams.getAll(name);

  return params.length > 0 ? params : defaultValue;
}

/**
 * Determine if a string is a valid date representation.
 */
export function isDate(value: string): boolean {
  const dateParsed = new Date(Date.parse(value));
  if (isNaN(dateParsed.getTime())) {
    return false;
  }
  const isoString = dateParsed.toISOString();
  return isoString === value;
}

/**
 * Determine if a value is a valid number.
 */
export function isNumber(value: unknown): value is number {
  return (
    value !== undefined &&
    value !== null &&
    value !== "" &&
    !isNaN(Number(value.toString()))
  );
}

/** Utility function to check if a given value is an object. */
export function isObject(value: unknown): value is object {
  // Returns true if the value is an object and not null.
  return typeof value === "object" && value !== null;
}

export function sumPostgresIntervalMs(interval: IPostgresInterval) {
  return (
    interval.days * 24 * 60 * 60 * 1000 +
    interval.hours * 60 * 60 * 1000 +
    interval.minutes * 60 * 1000 +
    interval.seconds * 1000 +
    interval.milliseconds
  );
}
