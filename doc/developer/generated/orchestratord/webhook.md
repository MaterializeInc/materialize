---
source: src/orchestratord/src/webhook.rs
revision: c7be3b0b51
---

# mz-orchestratord::webhook

Axum router implementing the Kubernetes CRD conversion webhook for Materialize custom resources.

`router()` mounts two routes: `POST /convert` (handled by `post_convert`) and `GET /healthz` (returns 200 OK).

`post_convert` accepts a `ConversionReview` JSON body, extracts the `ConversionRequest`, and calls `convert` on each object in the request's object list, returning a `ConversionReview` with a success or failure `ConversionResponse`.

`SupportedVersion` is a private enum covering `V1alpha1` (`materialize.cloud/v1alpha1`) and `V1` (`materialize.cloud/v1`). `convert` delegates cross-version conversions to `convert_v1alpha1_to_v1` and `convert_v1_to_v1alpha1` module functions (from `mz_cloud_resources::crd::materialize`) instead of directly deserializing into a typed `v1alpha1::Materialize` or `v1::Materialize` and re-serializing. These functions perform field-wise conversion over partial mirror structs, enabling correct conversion of partial objects sent by `kubectl apply --server-side`. Same-version conversions are identity; cross-version conversions that fail return a 500 with the error message. All conversions are traced at debug level; failures are also logged at warn.
