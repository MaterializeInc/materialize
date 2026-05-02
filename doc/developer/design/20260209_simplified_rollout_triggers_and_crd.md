# Simplified upgrade rollout triggers and CRD changes

- Associated: https://linear.app/materializeinc/issue/DEP-7/design-simplifying-upgrade-rollouts-node-rolls-converted-to-project

## The Problem

**Manual rollout triggering**

Users must manually set `requestRollout` to a new UUID value to trigger a rollout, even when they've already made meaningful changes to the spec.

- This does not match the behavior of anything else in Kubernetes.
- This is confusing for users, making mistakes extremely likely and increasing frustration.
- This makes using tools such as Terraform more difficult, as we need to also update this UUID, but not if the user has overridden it.
- This only exists to prevent us from updating Materialize instances outside a maintenance window or when orchestratord changes.

Additionally, the current system is difficult to automate when faced with eviction from nodes.

## Success Criteria

1. **Automatic rollout detection**: The system should automatically detect when a rollout is needed based on spec changes, without requiring users to manually set a UUID.

2. **Seamless version migration**: Existing v1alpha1 resources should continue to work, with automatic conversion to v1alpha2 as needed.

3. **Terraform compatibility**: Configuration must not fight with infrastructure as code tools such as Terraform.

4. **SaaS compatibility**: Updating orchestratord should not trigger a rollout if the Materialize CR is unchanged.

5. **Does not prevent implementation of node eviction handling**: While it is not required to have automation to handle node eviction, the implementation should not prevent adding support for node eviction later, without requiring additional major CRD changes.

## Out of Scope

- Rollback support.
- Node eviction handlers.

## Solution Proposal

### 1. New CRD Version: v1alpha2

Introduce a new `v1alpha2` version of the Materialize CRD with the following changes:

**Spec changes:**
- Remove `requestRollout` (`Uuid`) - Rollouts are now triggered automatically when the spec hash changes.
- Remove `inPlaceRollout` (`bool`) - This has been deprecated/ignored for a while, and is replaced with `MaterializeRolloutStrategy::ImmediatelyPromoteCausingDowntime`.
- Remove `environmentdIamRoleArn` (`Option<String>`) - This has been deprecated for a while, and is replaced with setting `"eks.amazonaws.com/role-arn"` in `serviceAccountAnnotations` instead. The conversion webhook should move this if it exists, with any conflicting value already present in `serviceAccountAnnotations` taking precedence.
- Change `forcePromote` from `Uuid` to `Option<String>` - Instead of triggering promotion when matching the UUID of `requestRollout`, it triggers promotion when matching the hash stored in `status.requestedRolloutHash`.

**Status changes:**
- Replace `lastCompletedRolloutRequest` (`Uuid`) with `lastCompletedRolloutHash` (`Option<String>`) - Stores the spec hash of the last successful rollout. Will be `None` if first deploying.
- Replace `resourcesHash` (`String`) with `requestedRolloutHash` (`Option<String>`) - Stores the spec hash of the currently requested rollout. Will be `None` when migrating from v1alpha1 while already in mid-rollout and in "promoting" status.

**Important Note!!!**
We **must not** update `requestedRolloutHash` if a rollout has reached the "promoting" state. At this point we have committed to promoting the current rollout, and do not want to trigger another one until it is complete. After the existing rollout has successfully promoted, another reconciliation will be triggered at which point we will update the `requestedRolloutHash` and trigger a new rollout.

### 2. Spec Hash Generation

A new `generate_rollout_hash()` method computes a SHA256 hash of the spec fields that affect rollouts:

```rust
pub fn generate_rollout_hash(&self) -> String {
    let mut hasher = Sha256::new();
    // Hash only fields that should trigger a rollout.
    // Excludes: balancerd/console resources, forcePromote, certificates
    // Exclusions are omitted here for brevity.
    let mut value = serde_json::to_value(&spec).unwrap();
    value.sort_all_objects();
    hasher.update(&serde_json::to_vec(&value).unwrap());
    // Include force_rollout annotation for manual triggers.
    // This is future planning so we can trigger rollouts without conflicting
    // with terraform-managed fields.
    if let Some(annotation) = self.metadata.annotations
        .and_then(|a| a.get(FORCE_ROLLOUT_ANNOTATION)) {
        hasher.update(annotation);
    }
    format!("{:x}", hasher.finalize())
}
```

Fields **excluded** from the hash (changes don't trigger rollout):
- `balancerdExternalCertificateSpec`
- `balancerdReplicas`
- `balancerdResourceRequirements`
- `consoleExternalCertificateSpec`
- `consoleReplicas`
- `consoleResourceRequirements`,
- `forcePromote`

These balancerd and console fields are excluded since they are applied on every reconciliation, without a rollout.

The `forcePromote` field is excluded, since it is used to promote the existing generation, and we don't want to tear that down every time it changes.

Fields **included** in the hash (changes trigger rollout):
- `environmentdImageRef`
- `environmentdExtraArgs`
- `environmentdExtraEnv`
- `environmentdConnectionRoleArn`
- `environmentdResourceRequirements`
- `environmentdScratchVolumeStorageRequirement`
- `serviceAccountName`
- `serviceAccountAnnotations`
- `serviceAccountLabels`
- `podAnnotations`
- `podLabels`
- `forceRollout`
- `rolloutStrategy`
- `backendSecretName`
- `authenticatorKind`
- `enableRbac`
- `environmentId`
- `systemParameterConfigmapName`
- `internalCertificateSpec`
- `materialize.cloud/force-rollout` annotation

All other spec fields, plus our force-rollout annotation.

Some of these are applied without requiring a rollout, but may require a rollout for some of their effects.
For example, `serviceAccountAnnotations` may be used to configure the AWS IAM role ARN, but it is unclear if that gets applied to existing pods.

### 3. Conversion Webhook

A new HTTPS webhook server handles CRD version conversion:

**Endpoint:** `POST /convert`

**Supported conversions:**
- v1alpha1 -> v1alpha2
- v1alpha2 -> v1alpha1\*

\*The API server seemed to want this, I don't know why. We can't reconcile these, so going back never makes sense.

**Key conversion logic:**

###### v1alpha1 to v1alpha2:
- Spec fields:
    - `forcePromote: Uuid` becomes `forcePromote: Option<String>` (nil UUID becomes None)
    - `requestRollout` is removed.
- Status fields:
    - `lastCompletedRolloutRequest` and `resourcesHash` are removed.
    - `conditions` are kept as-is.
    - If `lastCompletedRolloutRequest` and `spec.requestRollout` match:
        - This means we weren't in the middle of a rollout.
        - `lastCompletedRolloutHash` and `requestedRolloutHash` should both be set to the calculated hash (after conversion). This should avoid triggering a rollout during the migration.
    - Else:
        - `lastCompletedRolloutHash` should be set to `None` and `requestedRolloutHash` should be set to the calculated hash (after conversion). In this case, we likely have an in-progress rollout, which we will destroy and replace.
        - If we are already in "promoting" status, we should unconditionally complete the promotion for the current rollout rather than destroying and replacing it.
            This may trigger an additional rollout this one time, but I don't know any way around that. I think this is acceptable given the user is doing something very weird by updating orchestratord mid-rollout.

###### v1alpha2 to v1alpha1:

We need to include the `lastCompletedRolloutHash` from v1alpha2 in v1alpha1 as well. This is required for round tripping from v1alpha2 -> v1alpha1 -> v1alpha2,
which may happen if a user applies a v1alpha1 change over a v1alpha2 object.

In the case there is an existing `lastCompletedRolloutHash`, it should be kept as-is through the round trip. As we never reconcile with v1alpha1, it should only change at v1alpha2, so this should be safe.

No attempt is made to support v1alpha1 beyond giving a valid v1alpha1 structure and supporting round tripping to v1alpha2. Fields that do not exist in v1alpha2 may have their nil value.

##### Example round trips

In these examples, we assume that orchestratord's attempt to update the stored version succeeds and that reconciliation is triggered after this update. This is only to simplify this document, and is not necessary for correctness. If orchestratord's attempt to update the stored version fails, or the reconciliation is triggered first, the conversion webhook is simply called at that time and we will reconcile the same v1alpha2 object.

###### Simplest case
1. There is a stored v1alpha1 Materialize resource, not actively rolling out, with both `status.lastCompletedRolloutRequest` and `spec.requestRollout` matching.
1. Orchestratord gets updated to a version with v1alpha2 support.
1. Orchestratord lists existing v1alpha1 resources on startup, in order to upgrade them to v1alpha2.
    1. The API server calls the conversion webhook, which returns a v1alpha2 resource. In this case, it would have `status.lastCompletedRolloutHash` and `status.requestedRolloutHash` set to the same calculated hash after conversion.
1. Orchestratord calls `replace` to store the resource as v1alpha2.
1. Orchestratord gets notified of the new v1alpha2 resource, but determines there is nothing to do.

At this point, the stored version is v1alpha2, and no rollout is triggered.

1. The user then applies a v1alpha1 resource. It contains some change that affects the hash (ie: `spec.environmentd_image_ref`). It may or may not include `spec.requestRollout`, that doesn't matter.
1. Before storing this change, the API server calls the conversion webhook, which returns a v1alpha2 resource. In this case, it should not contain a status, as the user applied v1alpha1 resource did not contain a status (TODO verify this).
1. Orchestratord gets notified of the new v1alpha2 resource, which contains the old status not yet updated after the applied v1alpha1 resource. This means the `status.lastCompletedRolloutHash` and `status.requestedRolloutHash` still match each other, but do not match the calculated hash.
1. Orchestratord reconciles like normal, calculating a new `status.requestedRolloutHash` and triggering a rollout since it is different.

If the user had instead applied a v1alpha2 resource instead, no conversion would be needed and orchestratord would reconcile it directly.

###### Existing v1alpha1 resource is mid-upgrade, but not promoting
1. There is a stored v1alpha1 Materialize resource, actively rolling out, with `status.lastCompletedRolloutRequest` and `spec.requestRollout` not matching. It is not in "promoting" status.
1. Orchestratord gets updated to a version with v1alpha2 support.
1. Orchestratord lists existing v1alpha1 resources on startup, in order to upgrade them to v1alpha2.
    1. The API server calls the conversion webhook, which returns a v1alpha2 resource. In this case, it would have `status.lastCompletedRolloutHash` set to `None` and `status.requestedRolloutHash` set to the calculated hash after conversion.
1. Orchestratord calls `replace` to store the resource as v1alpha2.
1. Orchestratord gets notified of the new v1alpha2 resource.
1. Orchestratord reconciles like normal, continuing the existing rollout and overwriting any objects that are different. This is the same behavior it would have with current orchestratord and v1alpha1.

###### Existing v1alpha1 resource is mid-upgrade and already promoting
1. There is a stored v1alpha1 Materialize resource, actively rolling out, with `status.lastCompletedRolloutRequest` and `spec.requestRollout` not matching. It is in "promoting" status.
1. Orchestratord gets updated to a version with v1alpha2 support.
1. Orchestratord lists existing v1alpha1 resources on startup, in order to upgrade them to v1alpha2.
    1. The API server calls the conversion webhook, which returns a v1alpha2 resource. In this case, it would have `status.lastCompletedRolloutHash` set to `None` and `status.requestedRolloutHash` set to the calculated hash after conversion.
1. Orchestratord calls `replace` to store the resource as v1alpha2.
1. Orchestratord gets notified of the new v1alpha2 resource.
1. Orchestratord reconciles like normal. Critically, it unconditionally continues with promotion rather than overwriting any objects.
1. After promotion is successful, the updated status triggers a new rollout. (TODO verify that this works if we have a `status.requestedRolloutHash` set in the initial conversion)

### 4. Helm Chart Changes

Conversion webhooks only support HTTPS, so we need a certificate for orchestratord. This may also be useful in the future, for admission webhooks for example.

We plan to support two options:
- Automatic generation using cert-manager.
    - Creates a self-signed `Issuer` using cert-manager
    - Creates a `Certificate` with the service DNS name, using Ed25519 algorithm, with rotation enabled.
    - TODO implement reloading when it changes.
- User supplied certificates in a secret, containing fields matching the what we would get from cert-manager.

Which of these is used is determined by a new helm value `operator.certificate.source`, which can be either "cert-manager" (the default) or "secret".

If `operator.certificate.source` is set to "secret", the user must also set `operator.certificate.secretName` to the name of a secret in the operator's namespace.

Regardless of which they choose, the resulting secret will be mounted into the orchestratord pods.

Orchestratord will also get readiness probes so nothing tries to call this webhook before it is up.


### 5. CRD Registration

The CRD is registered with:
- Both v1alpha1 and v1alpha2 versions
- v1alpha2 as the stored version
- Webhook conversion configuration pointing to the operator service

```rust
mz_crd.spec.conversion = Some(CustomResourceConversion {
    strategy: "Webhook".to_owned(),
    webhook: Some(WebhookConversion {
        client_config: Some(WebhookClientConfig {
            ca_bundle: Some(ByteString(ca_bytes)),
            service: Some(ServiceReference {
                name: webhook_service_name,
                namespace: webhook_service_namespace,
                path: Some("/convert".to_owned()),
                port: Some(webhook_service_port.into()),
            }),
            url: None,
        }),
        conversion_review_versions: vec!["v1".to_owned()],
    }),
});
```

### 6. Replace all Materialize resources to update their stored versions

We have set v1alpha2 as the stored version, but that doesn't update existing resources. Those are only updated when they are reapplied.

During orchestratord startup, after waiting for the CRD to be established, we need to loop through all Materialize resources and `replace` them.

If it is possible to determine the stored version of these resources, we should only `replace` the ones at the older version.

I think it is OK for this to be best-effort, and only warn in case of failure.
For backward compatibility reasons, we're going to have to support the old version for some time.
Orchestratord is likely to get restarted/upgraded multiple times in that period, so it can try again.
If the user ever writes an updated CR, it will also be stored in v1alpha2, so it isn't critical that this work immediately.

## Known testing required

Our existing nightly orchestratord tests cover a lot, but we'll need to extend them to work with multiple CRD versions.

- Upgrades from existing v1alpha1 environments by applying v1alpha1 CR. (this is basically what we have now, but we need to not break it with the orchestratord changes to reconcile v1alpha2 after conversion)
- Upgrades from existing v1alpha1 environments by applying v1alpha2 CR.
- Upgrades from existing v1alpha2 environments by applying v1alpha1 CR.
- Upgrades from existing v1alpha2 environments by applying v1alpha2 CR.
- Upgrade from existing v1alpha1 environment that is mid-rollout not in "promoting" status.
- Upgrade from existing v1alpha1 environment that is mid-rollout in "promoting" status.
- Upgrades with a previous rollout already in progress.
- Upgrades triggered by annotation.
- Deploy of latest Materialize image versions using v1alpha2 CR.
- Deploy of older Materialize image versions using v1alpha2 CR.

## Minimal Viable Prototype

Still a work in progress: https://github.com/MaterializeInc/materialize/pull/34904

## Alternatives

### 1. Keep using our current v1alpha1 CRD as-is

Continue using the hash of generated Kubernetes resources rather than the Materialize spec. Maybe changing this will be more problematic than our current issues with it.

**Reasons not chosen:** The resource hash includes implementation details that may change without user intent (e.g., pod labels, spec fields, additional services, etc..), causing unexpected rollouts. Hashing the spec directly is more predictable. We still have all the problems we have now.

### 2. In-place CRD migration without versioning

Modify v1alpha1 directly rather than creating a new version.

**Reasons not chosen:** This would likely break existing deployments. Kubernetes CRD versioning with conversion webhooks is the standard approach for API evolution, and we need to be able to evolve our CRDs in backwards incompatible ways. I don't know of any backwards compatible schema that would solve our problems here. *I'm open to suggestions if you know a way.* Making many fields optional and deprecated gets confusing very quickly.

### 3. Date based triggers for forcePromote and forceRollout

Mostly the same as this proposal, but changing the types of `forcePromote` and `forceRollout` to be either an i64 of seconds since the unix epoch, or a UTC ISO datetime string. This would allow us to set both of these to the same value and trigger a rollout and immediately force promotion. We'd also need to store the initial timestamp of the current rollout (`currentRolloutTimestamp`) and the last completed rollout (`lastCompletedRolloutTimestamp`) in the status (if they exist), for comparison.

When initially updating the status when starting a rollout, orchestratord would have logic like the following for determining which timestamp to use:
```
# treat all non-existant values as older
manuallyTriggeredTimestamp = max(spec.forceRollout, forceRolloutAnnotation)
if manuallyTriggeredTimestamp > lastCompletedRolloutTimestamp
    if manuallyTriggeredTimestamp > currentRolloutTimestamp:
        return manuallyTriggeredTimestamp
return now()
```

**Reasons not chosen:** This timestamp logic is complex and likely to introduce errors. The gains of being able to predict the `forcePromote` value seem minimal.

## Open questions

1. **Backwards compatibility period:**
How long should we support v1alpha1 before removing it?
The conversion webhook enables indefinite support, but maintaining both versions has a cost.

2. **Certificate rotation:**
The current implementation uses cert-manager's automatic rotation, but we haven't tested long-running deployments through certificate renewal cycles, or implemented reloading in orchestratord.

3. **Is it OK to require setting forcePromote in a separate modification?**
We can't easily know the rollout hash in advance, so users can't set `forcePromote` in the same apply as other changes.
I think this is probably fine, since `forcePromote` only really makes sense if the rollout is stuck, which won't be the case when initially updating the spec.

4. **Do we need to optionally disable triggering updates on spec changes in SaaS?**
We may also need to add a new field to the Materialize CRD spec for whether to trigger updates immediately on spec changes, or if the annotation must trigger them.
This lets us not trigger updates immediately in SaaS when we update the region-controller to change the Materialize CR.
We don't change the region-controller often, and we can always update it to not apply changes to the Materialize resource until we trigger that, so this probably isn't necessary.

5. **Interface changes for helm values**
What I've got in this proposal probably works, but I'm not sure if it's exactly what we want to lock ourselves to.

6. **Should we introduce an intermediate generation object?**
A generation object (placeholder name) similar to ReplicaSets for Deployments could simplify some of our reconciliation logic.
It seemed like a lot to bite off at the same time as these other changes, though.
I'm not sure if it will be backwards compatible to add it later.
I think it will, but am not certain.
If we want generation objects, should we consider doing that first?
This is probably fine either way. We can always do another CRD migration if it turns out to not be compatible with this one.
