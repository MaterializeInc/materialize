---
source: src/orchestratord/src/webhook.rs
revision: ff4065dc30
---

# mz-orchestratord::webhook

Axum router implementing the Kubernetes CRD conversion webhook for Materialize custom resources.

`router()` mounts two routes: `POST /convert` (handled by `post_convert`) and `GET /healthz` (returns 200 OK).

`post_convert` accepts a `ConversionReview` JSON body, extracts the `ConversionRequest`, and calls `convert` on each object in the request's object list, returning a `ConversionReview` with a success or failure `ConversionResponse`.

`SupportedVersion` is a private enum covering `V1alpha1` (`materialize.cloud/v1alpha1`) and `V1` (`materialize.cloud/v1`). `convert` deserializes each object into the source version's type and re-serializes it into the desired version using the `From` impls between `v1alpha1::Materialize` and `v1::Materialize`. Same-version conversions are identity; cross-version conversions that fail deserialization or serialization return a 500 with the error message. All conversions are traced at debug level; failures are also logged at warn.
