// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export type MimeType = "text/plain" | "text/csv";
export type DownloadFileOptions = {
  mimeType: MimeType;
};

/**
 * Trigger a download of a file to the local machine.
 *
 * @param fileName the default file name
 * @param contents the file contents
 * @param options (optional) additional properties for the file
 */
export function downloadFile(
  fileName: string,
  contents: string,
  options: DownloadFileOptions = { mimeType: "text/plain" },
) {
  const file = new Blob([contents], { type: options.mimeType });
  const url = URL.createObjectURL(file);
  const element = document.createElement("a");
  element.href = url;
  element.download = fileName;
  document.body.appendChild(element);
  element.click();
  // Cleanup
  element.remove();
  URL.revokeObjectURL(url);
}
