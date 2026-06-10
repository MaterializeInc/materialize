// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

const scriptSrc = [
  "'self'",
  // Frontegg scripts
  "https://*.frontegg.com",
  "https://cdn.segment.com",
  "https://vercel.live/",
  "https://vercel.com",
  // Used for Vercel preview links
  "'unsafe-inline'",
  // hubspot tracking code
  "https://*.hsadspixel.net",
  "https://*.hubspot.com",
  "https://*.hscollectedforms.net",
  "https://*.hs-analytics.net",
  "https://*.hs-banner.com",
  "https://*.hubspotfeedback.com",
  "https://*.hs-scripts.com",
  "https://*.googletagmanager.com",
  "https://accounts.google.com",
  // stripe script-src
  "https://*.js.stripe.com",
  "https://js.stripe.com",
  "https://maps.googleapis.com",

  // Intercom
  "https://app.intercom.io",
  "https://widget.intercom.io",
  "https://js.intercomcdn.com",
];

const intercomChildSrc = [
  "https://intercom-sheets.com",
  "https://www.intercom-reporting.com",
  "https://www.youtube.com",
  "https://player.vimeo.com",
  "https://fast.wistia.net",
];

const intercomImageSrc = [
  "https://js.intercomcdn.com",
  "https://static.intercomassets.com",
  "https://downloads.intercomcdn.com",
  "https://downloads.intercomcdn.eu",
  "https://downloads.au.intercomcdn.com",
  "https://uploads.intercomusercontent.com",
  "https://gifs.intercomcdn.com ",
  "https://video-messages.intercomcdn.com",
  "https://messenger-apps.intercom.io",
  "https://messenger-apps.eu.intercom.io",
  "https://messenger-apps.au.intercom.io",
  "https://*.intercom-attachments-1.com",
  "https://*.intercom-attachments.eu",
  "https://*.au.intercom-attachments.com",
  "https://*.intercom-attachments-2.com",
  "https://*.intercom-attachments-3.com",
  "https://*.intercom-attachments-4.com",
  "https://*.intercom-attachments-5.com",
  "https://*.intercom-attachments-6.com",
  "https://*.intercom-attachments-7.com",
  "https://*.intercom-attachments-8.com",
  "https://*.intercom-attachments-9.com",
  "https://static.intercomassets.eu",
  "https://static.au.intercomassets.com",
  "https://res.cloudinary.com",
  "https://incident.io",
  "https://img.youtube.com",
];

export const csp = {
  "base-uri": ["'self'"],
  "child-src": intercomChildSrc,
  "connect-src": ["*"],
  "default-src": ["'self'"],
  "form-action": [
    // Intercom
    "https://intercom.help",
    "https://api-iam.intercom.io",
    "https://api-iam.eu.intercom.io",
    "https://api-iam.au.intercom.io",
  ],
  "font-src": [
    "'self'",
    "data:",
    "fonts.gstatic.com/",
    "fonts.googleapis.com/",
    "https://vercel.live/",
    // Intercom
    "https://js.intercomcdn.com",
    "https://fonts.intercomcdn.com",
  ],
  "frame-src": [
    // Allow PDFs from Materialize to be iframed
    "https://materialize.com/",
    "https://vercel.live/",
    "https://vercel.com",
    // hubspot tracking code
    "*.hubspot.com",
    // stripe iframe
    "https://*.js.stripe.com",
    "https://js.stripe.com",
    "https://hooks.stripe.com",
  ],
  "img-src": [
    "'self'",
    "blob:",
    "data:",
    // Github profile images
    "https://avatars.githubusercontent.com/",
    // Google profile images
    "https://lh3.googleusercontent.com",
    // Frontegg fallback profile images
    "https://www.gravatar.com",
    "https://*.wp.com",
    // Frontegg production profile images
    "https://fronteggprodeustorage.blob.core.windows.net",
    // Frontegg images
    "https://*.frontegg.com",
    "https://vercel.live/",
    "https://vercel.com",
    // hubspot tracking code
    "https://*.hsforms.com",
    "https://*.hubspot.com",
    ...intercomImageSrc,
  ],
  "media-src": [
    "'self'",
    "https://de62f3dtfyis.cloudfront.net",
    // Intercom
    "https://js.intercomcdn.com",
    "https://downloads.intercomcdn.com",
    "https://downloads.intercomcdn.eu",
    "https://downloads.au.intercomcdn.com",
  ],
  "object-src": ["'none'"],
  "script-src": ["'wasm-unsafe-eval'", ...scriptSrc],
  "script-src-elem": scriptSrc,
  "style-src": [
    "'self'",
    "'unsafe-inline'",
    // Frontegg admin panel
    "fonts.googleapis.com",
    "https://*.frontegg.com",
    "https://vercel.live",
  ],
  "worker-src": ["'none'"],
};

/**
 * @param {string | undefined} sentryRelease
 * @param {string | undefined} sentryEnvironment
 */
export function buildCsp(sentryRelease, sentryEnvironment) {
  const builtCsp = structuredClone(csp);
  if (sentryEnvironment !== undefined && sentryRelease !== undefined) {
    builtCsp["report-uri"] = [
      `https://o561021.ingest.us.sentry.io/api/5699757/security/?sentry_key=13c8b3a8d1e547c9b9493de997b04337&sentry_environment=${sentryEnvironment}&sentry_release=${sentryRelease}`,
    ];
  }
  const directives = Object.entries(builtCsp).map(
    ([key, values]) => `${key} ${values.join(" ")}`,
  );

  return directives.join("; ");
}
