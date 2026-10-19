// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/** The base of every error the SDK throws. */
export class SdkError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** The server broke the `SUBSCRIBE` protocol, a bug in Materialize or the SDK. */
export class ProtocolError extends SdkError {}

/** A timestamp's updates exceeded the memory limit. */
export class TimestampTooLarge extends SdkError {}

/** The SDK was called wrongly. */
export class UsageError extends SdkError {}

/** The server or the connection failed, with the core's classification. */
export class ServerError extends SdkError {
  constructor(
    message: string,
    readonly sqlstate: string | undefined,
    readonly classification: Record<string, unknown>,
  ) {
    super(message);
  }
}
