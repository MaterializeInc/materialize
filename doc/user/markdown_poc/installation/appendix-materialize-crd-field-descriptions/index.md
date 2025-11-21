<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [Install/Upgrade (Self-Managed)](/docs/installation/)

</div>

# Appendix: Materialize CRD Field Descriptions

#### MaterializeSpec

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>backendSecretName</code></td>
<td>✅</td>
<td><em><strong>String</strong></em>
<p>The name of a secret containing <code>metadata_backend_url</code> and
<code>persist_backend_url</code>. It may also contain
<code>external_login_password_mz_system</code>, which will be used as
the password for the <code>mz_system</code> user if
<code>authenticatorKind</code> is <code>Password</code>.</p></td>
</tr>
<tr>
<td><code>environmentdImageRef</code></td>
<td>✅</td>
<td><em><strong>String</strong></em>
<p>The environmentd image to run.</p></td>
</tr>
<tr>
<td><code>authenticatorKind</code></td>
<td></td>
<td><em><strong>Enum</strong></em>
<p>How to authenticate with Materialize.</p>
<p>Valid values:</p>
<ul>
<li><code>Frontegg</code>:<br />
Authenticate users using Frontegg.</li>
<li><code>Password</code>:<br />
Authenticate users using internally stored password hashes. The backend
secret must contain external_login_password_mz_system.</li>
<li><code>Sasl</code>:<br />
Authenticate users using SASL.</li>
<li><code>None</code> (default):<br />
Do not authenticate users. Trust they are who they say they are without
verification.</li>
</ul>
<p><strong>Default:</strong> <code>None</code></p></td>
</tr>
<tr>
<td><code>balancerdExternalCertificateSpec</code></td>
<td></td>
<td><em><strong><a
href="#materializecertspec">MaterializeCertSpec</a></strong></em>
<p>The configuration for generating an x509 certificate using
cert-manager for balancerd to present to incoming connections. The
<code>dnsNames</code> and <code>issuerRef</code> fields are
required.</p></td>
</tr>
<tr>
<td><code>balancerdReplicas</code></td>
<td></td>
<td><em><strong>Integer</strong></em>
<p>Number of balancerd pods to create.</p></td>
</tr>
<tr>
<td><code>balancerdResourceRequirements</code></td>
<td></td>
<td><em><strong><a
href="#iok8sapicorev1resourcerequirements">io.k8s.api.core.v1.ResourceRequirements</a></strong></em>
<p>Resource requirements for the balancerd pod.</p></td>
</tr>
<tr>
<td><code>consoleExternalCertificateSpec</code></td>
<td></td>
<td><em><strong><a
href="#materializecertspec">MaterializeCertSpec</a></strong></em>
<p>The configuration for generating an x509 certificate using
cert-manager for the console to present to incoming connections. The
<code>dnsNames</code> and <code>issuerRef</code> fields are required.
Not yet implemented.</p></td>
</tr>
<tr>
<td><code>consoleReplicas</code></td>
<td></td>
<td><em><strong>Integer</strong></em>
<p>Number of console pods to create.</p></td>
</tr>
<tr>
<td><code>consoleResourceRequirements</code></td>
<td></td>
<td><em><strong><a
href="#iok8sapicorev1resourcerequirements">io.k8s.api.core.v1.ResourceRequirements</a></strong></em>
<p>Resource requirements for the console pod.</p></td>
</tr>
<tr>
<td><code>enableRbac</code></td>
<td></td>
<td><em><strong>Bool</strong></em>
<p>Whether to enable role based access control. Defaults to
false.</p></td>
</tr>
<tr>
<td><code>environmentId</code></td>
<td></td>
<td><em><strong>Uuid</strong></em>
<p>The value used by environmentd (via the –environment-id flag) to
uniquely identify this instance. Must be globally unique, and is
required if a license key is not provided. NOTE: This value MUST NOT be
changed in an existing instance, since it affects things like the way
data is stored in the persist backend.</p>
<p><strong>Default:</strong>
<code>00000000-0000-0000-0000-000000000000</code></p></td>
</tr>
<tr>
<td><code>environmentdConnectionRoleArn</code></td>
<td></td>
<td><em><strong>String</strong></em>
<p>If running in AWS, override the IAM role to use to support the CREATE
CONNECTION feature.</p></td>
</tr>
<tr>
<td><code>environmentdExtraArgs</code></td>
<td></td>
<td><em><strong>Array&lt;String&gt;</strong></em>
<p>Extra args to pass to the environmentd binary.</p></td>
</tr>
<tr>
<td><code>environmentdExtraEnv</code></td>
<td></td>
<td><em><strong>Array&lt;<a
href="#iok8sapicorev1envvar">io.k8s.api.core.v1.EnvVar</a>&gt;</strong></em>
<p>Extra environment variables to pass to the environmentd
binary.</p></td>
</tr>
<tr>
<td><code>environmentdResourceRequirements</code></td>
<td></td>
<td><em><strong><a
href="#iok8sapicorev1resourcerequirements">io.k8s.api.core.v1.ResourceRequirements</a></strong></em>
<p>Resource requirements for the environmentd pod.</p></td>
</tr>
<tr>
<td><code>environmentdScratchVolumeStorageRequirement</code></td>
<td></td>
<td><em><strong>io.k8s.apimachinery.pkg.api.resource.Quantity</strong></em>
<p>Amount of disk to allocate, if a storage class is provided.</p></td>
</tr>
<tr>
<td><code>forcePromote</code></td>
<td></td>
<td><em><strong>Uuid</strong></em>
<p>If <code>forcePromote</code> is set to the same value as
<code>requestRollout</code>, the current rollout will skip waiting for
clusters in the new generation to rehydrate before promoting the new
environmentd to leader.</p>
<p><strong>Default:</strong>
<code>00000000-0000-0000-0000-000000000000</code></p></td>
</tr>
<tr>
<td><code>forceRollout</code></td>
<td></td>
<td><em><strong>Uuid</strong></em>
<p>This value will be written to an annotation in the generated
environmentd statefulset, in order to force the controller to detect the
generated resources as changed even if no other changes happened. This
can be used to force a rollout to a new generation even without making
any meaningful changes, by setting it to the same value as
<code>requestRollout</code>.</p>
<p><strong>Default:</strong>
<code>00000000-0000-0000-0000-000000000000</code></p></td>
</tr>
<tr>
<td><code>internalCertificateSpec</code></td>
<td></td>
<td><em><strong><a
href="#materializecertspec">MaterializeCertSpec</a></strong></em>
<p>The cert-manager Issuer or ClusterIssuer to use for database internal
communication. The <code>issuerRef</code> field is required. This
currently is only used for environmentd, but will eventually support
clusterd.</p></td>
</tr>
<tr>
<td><code>podAnnotations</code></td>
<td></td>
<td><em><strong>Map&lt;String, String&gt;</strong></em>
<p>Annotations to apply to the pods.</p></td>
</tr>
<tr>
<td><code>podLabels</code></td>
<td></td>
<td><em><strong>Map&lt;String, String&gt;</strong></em>
<p>Labels to apply to the pods.</p></td>
</tr>
<tr>
<td><code>requestRollout</code></td>
<td></td>
<td><em><strong>Uuid</strong></em>
<p>When changes are made to the environmentd resources (either via
modifying fields in the spec here or by deploying a new orchestratord
version which changes how resources are generated), existing
environmentd processes won’t be automatically restarted. In order to
trigger a restart, the request_rollout field should be set to a new
(random) value. Once the rollout completes, the value of
<code>status.lastCompletedRolloutRequest</code> will be set to this
value to indicate completion.</p>
<p>Defaults to a random value in order to ensure that the first
generation rollout is automatically triggered.</p>
<p><strong>Default:</strong>
<code>00000000-0000-0000-0000-000000000000</code></p></td>
</tr>
<tr>
<td><code>rolloutStrategy</code></td>
<td></td>
<td><em><strong>Enum</strong></em>
<p>Rollout strategy to use when upgrading this Materialize instance.</p>
<p>Valid values:</p>
<ul>
<li><p><code>WaitUntilReady</code> (default):<br />
Create a new generation of pods, leaving the old generation around until
the new ones are ready to take over. This minimizes downtime, and is
what almost everyone should use.</p></li>
<li><p><code>ImmediatelyPromoteCausingDowntime</code>:<br />
</p>
<div class="warning">
<strong>WARNING!</strong>
<p>THIS WILL CAUSE YOUR MATERIALIZE INSTANCE TO BE UNAVAILABLE FOR SOME
TIME!!!</p>
<p>This strategy should ONLY be used by customers with physical hardware
who do not have enough hardware for the <code>WaitUntilReady</code>
strategy. If you think you want this, please consult with Materialize
engineering to discuss your situation.</p>
</div>
<p>Tear down the old generation of pods and promote the new generation
of pods immediately, without waiting for the new generation of pods to
be ready.</p></li>
</ul>
<p><strong>Default:</strong> <code>WaitUntilReady</code></p></td>
</tr>
<tr>
<td><code>serviceAccountAnnotations</code></td>
<td></td>
<td><em><strong>Map&lt;String, String&gt;</strong></em>
<p>Annotations to apply to the service account.</p>
<p>Annotations on service accounts are commonly used by cloud providers
for IAM. AWS uses “eks.amazonaws.com/role-arn”. Azure uses
“azure.workload.identity/client-id”, but additionally requires
“azure.workload.identity/use”: “true” on the pods.</p></td>
</tr>
<tr>
<td><code>serviceAccountLabels</code></td>
<td></td>
<td><em><strong>Map&lt;String, String&gt;</strong></em>
<p>Labels to apply to the service account.</p></td>
</tr>
<tr>
<td><code>serviceAccountName</code></td>
<td></td>
<td><em><strong>String</strong></em>
<p>Name of the kubernetes service account to use. If not set, we will
create one with the same name as this Materialize object.</p></td>
</tr>
</tbody>
</table>

#### MaterializeCertSpec

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>dnsNames</code></td>
<td></td>
<td><em><strong>Array&lt;String&gt;</strong></em>
<p>Additional DNS names the certificate will be valid for.</p></td>
</tr>
<tr>
<td><code>duration</code></td>
<td></td>
<td><em><strong>String</strong></em>
<p>Duration the certificate will be requested for. Value must be in
units accepted by Go <a
href="https://golang.org/pkg/time/#ParseDuration"><code>time.ParseDuration</code></a>.</p></td>
</tr>
<tr>
<td><code>issuerRef</code></td>
<td></td>
<td><em><strong><a
href="#certificateissuerref">CertificateIssuerRef</a></strong></em>
<p>Reference to an <code>Issuer</code> or <code>ClusterIssuer</code>
that will generate the certificate.</p></td>
</tr>
<tr>
<td><code>renewBefore</code></td>
<td></td>
<td><em><strong>String</strong></em>
<p>Duration before expiration the certificate will be renewed. Value
must be in units accepted by Go <a
href="https://golang.org/pkg/time/#ParseDuration"><code>time.ParseDuration</code></a>.</p></td>
</tr>
<tr>
<td><code>secretTemplate</code></td>
<td></td>
<td><em><strong><a
href="#certificatesecrettemplate">CertificateSecretTemplate</a></strong></em>
<p>Additional annotations and labels to include in the Certificate
object.</p></td>
</tr>
</tbody>
</table>

#### CertificateSecretTemplate

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>annotations</code></td>
<td></td>
<td><em><strong>Map&lt;String, String&gt;</strong></em>
<p>Annotations is a key value map to be copied to the target Kubernetes
Secret.</p></td>
</tr>
<tr>
<td><code>labels</code></td>
<td></td>
<td><em><strong>Map&lt;String, String&gt;</strong></em>
<p>Labels is a key value map to be copied to the target Kubernetes
Secret.</p></td>
</tr>
</tbody>
</table>

#### CertificateIssuerRef

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>name</code></td>
<td>✅</td>
<td><em><strong>String</strong></em>
<p>Name of the resource being referred to.</p></td>
</tr>
<tr>
<td><code>group</code></td>
<td></td>
<td><em><strong>String</strong></em>
<p>Group of the resource being referred to.</p></td>
</tr>
<tr>
<td><code>kind</code></td>
<td></td>
<td><em><strong>String</strong></em>
<p>Kind of the resource being referred to.</p></td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.ResourceRequirements

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>claims</code></td>
<td></td>
<td><em><strong>Array&lt;<a
href="#iok8sapicorev1resourceclaim">io.k8s.api.core.v1.ResourceClaim</a>&gt;</strong></em>
<p>Claims lists the names of resources, defined in spec.resourceClaims,
that are used by this container.</p>
<p>This is an alpha field and requires enabling the
DynamicResourceAllocation feature gate.</p>
<p>This field is immutable. It can only be set for containers.</p></td>
</tr>
<tr>
<td><code>limits</code></td>
<td></td>
<td><em><strong>Map&lt;String,
io.k8s.apimachinery.pkg.api.resource.Quantity&gt;</strong></em>
<p>Limits describes the maximum amount of compute resources allowed.
More info: <a
href="https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/">https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/</a></p></td>
</tr>
<tr>
<td><code>requests</code></td>
<td></td>
<td><em><strong>Map&lt;String,
io.k8s.apimachinery.pkg.api.resource.Quantity&gt;</strong></em>
<p>Requests describes the minimum amount of compute resources required.
If Requests is omitted for a container, it defaults to Limits if that is
explicitly specified, otherwise to an implementation-defined value.
Requests cannot exceed Limits. More info: <a
href="https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/">https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/</a></p></td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.ResourceClaim

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>name</code></td>
<td>✅</td>
<td><em><strong>String</strong></em>
<p>Name must match the name of one entry in pod.spec.resourceClaims of
the Pod where this field is used. It makes that resource available
inside a container.</p></td>
</tr>
<tr>
<td><code>request</code></td>
<td></td>
<td><em><strong>String</strong></em>
<p>Request is the name chosen for a request in the referenced claim. If
empty, everything from the claim is made available, otherwise only the
result of this request.</p></td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.EnvVar

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>name</code></td>
<td>✅</td>
<td><em><strong>String</strong></em>
<p>Name of the environment variable. Must be a C_IDENTIFIER.</p></td>
</tr>
<tr>
<td><code>value</code></td>
<td></td>
<td><em><strong>String</strong></em>
<p>Variable references $(VAR_NAME) are expanded using the previously
defined environment variables in the container and any service
environment variables. If a variable cannot be resolved, the reference
in the input string will be unchanged. Double $$ are reduced to a single
$, which allows for escaping the $(VAR_NAME) syntax: i.e. “$$(VAR_NAME)”
will produce the string literal “$(VAR_NAME)”. Escaped references will
never be expanded, regardless of whether the variable exists or not.
Defaults to “”.</p></td>
</tr>
<tr>
<td><code>valueFrom</code></td>
<td></td>
<td><em><strong><a
href="#iok8sapicorev1envvarsource">io.k8s.api.core.v1.EnvVarSource</a></strong></em>
<p>Source for the environment variable’s value. Cannot be used if value
is not empty.</p></td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.EnvVarSource

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>configMapKeyRef</code></td>
<td></td>
<td><em><strong><a
href="#iok8sapicorev1configmapkeyselector">io.k8s.api.core.v1.ConfigMapKeySelector</a></strong></em>
<p>Selects a key of a ConfigMap.</p></td>
</tr>
<tr>
<td><code>fieldRef</code></td>
<td></td>
<td><em><strong><a
href="#iok8sapicorev1objectfieldselector">io.k8s.api.core.v1.ObjectFieldSelector</a></strong></em>
<p>Selects a field of the pod: supports metadata.name,
metadata.namespace, <code>metadata.labels['&lt;KEY&gt;']</code>,
<code>metadata.annotations['&lt;KEY&gt;']</code>, spec.nodeName,
spec.serviceAccountName, status.hostIP, status.podIP,
status.podIPs.</p></td>
</tr>
<tr>
<td><code>resourceFieldRef</code></td>
<td></td>
<td><em><strong><a
href="#iok8sapicorev1resourcefieldselector">io.k8s.api.core.v1.ResourceFieldSelector</a></strong></em>
<p>Selects a resource of the container: only resources limits and
requests (limits.cpu, limits.memory, limits.ephemeral-storage,
requests.cpu, requests.memory and requests.ephemeral-storage) are
currently supported.</p></td>
</tr>
<tr>
<td><code>secretKeyRef</code></td>
<td></td>
<td><em><strong><a
href="#iok8sapicorev1secretkeyselector">io.k8s.api.core.v1.SecretKeySelector</a></strong></em>
<p>Selects a key of a secret in the pod’s namespace</p></td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.SecretKeySelector

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>key</code></td>
<td>✅</td>
<td><em><strong>String</strong></em>
<p>The key of the secret to select from. Must be a valid secret
key.</p></td>
</tr>
<tr>
<td><code>name</code></td>
<td>✅</td>
<td><em><strong>String</strong></em>
<p>Name of the referent. This field is effectively required, but due to
backwards compatibility is allowed to be empty. Instances of this type
with an empty value here are almost certainly wrong. More info: <a
href="https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names">https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names</a></p></td>
</tr>
<tr>
<td><code>optional</code></td>
<td></td>
<td><em><strong>Bool</strong></em>
<p>Specify whether the Secret or its key must be defined</p></td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.ResourceFieldSelector

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>resource</code></td>
<td>✅</td>
<td><em><strong>String</strong></em>
<p>Required: resource to select</p></td>
</tr>
<tr>
<td><code>containerName</code></td>
<td></td>
<td><em><strong>String</strong></em>
<p>Container name: required for volumes, optional for env vars</p></td>
</tr>
<tr>
<td><code>divisor</code></td>
<td></td>
<td><em><strong>io.k8s.apimachinery.pkg.api.resource.Quantity</strong></em>
<p>Specifies the output format of the exposed resources, defaults to
“1”</p></td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.ObjectFieldSelector

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>fieldPath</code></td>
<td>✅</td>
<td><em><strong>String</strong></em>
<p>Path of the field to select in the specified API version.</p></td>
</tr>
<tr>
<td><code>apiVersion</code></td>
<td></td>
<td><em><strong>String</strong></em>
<p>Version of the schema the FieldPath is written in terms of, defaults
to “v1”.</p></td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.ConfigMapKeySelector

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>Field Name</th>
<th>Required</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td><code>key</code></td>
<td>✅</td>
<td><em><strong>String</strong></em>
<p>The key to select.</p></td>
</tr>
<tr>
<td><code>name</code></td>
<td>✅</td>
<td><em><strong>String</strong></em>
<p>Name of the referent. This field is effectively required, but due to
backwards compatibility is allowed to be empty. Instances of this type
with an empty value here are almost certainly wrong. More info: <a
href="https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names">https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names</a></p></td>
</tr>
<tr>
<td><code>optional</code></td>
<td></td>
<td><em><strong>Bool</strong></em>
<p>Specify whether the ConfigMap or its key must be defined</p></td>
</tr>
</tbody>
</table>

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/installation/appendix-materialize-crd-field-descriptions.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
