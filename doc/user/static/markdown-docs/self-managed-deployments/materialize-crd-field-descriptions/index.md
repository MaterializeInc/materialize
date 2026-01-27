# Materialize CRD Field Descriptions
Reference page on Materialize CRD Fields





























#### MaterializeSpec
<table>
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
<td>
<em><strong>String</strong></em>


<p>The name of a secret containing <code>metadata_backend_url</code> and <code>persist_backend_url</code>.
It may also contain <code>external_login_password_mz_system</code>, which will be used as
the password for the <code>mz_system</code> user if <code>authenticatorKind</code> is <code>Password</code>.</p>

</td>
</tr>
<tr>
<td><code>environmentdImageRef</code></td>
<td>✅</td>
<td>
<em><strong>String</strong></em>


<p>The environmentd image to run.</p>

</td>
</tr>
<tr>
<td><code>authenticatorKind</code></td>
<td></td>
<td>
<em><strong>Enum</strong></em>


<p><p>How to authenticate with Materialize.</p>
<p>Valid values:</p>
<ul>
<li><code>Frontegg</code>:<br>  Authenticate users using Frontegg.</li>
<li><code>Password</code>:<br>  Authenticate users using internally stored password hashes.
The backend secret must contain external_login_password_mz_system.</li>
<li><code>Sasl</code>:<br>  Authenticate users using SASL.</li>
<li><code>None</code> (default):<br>  Do not authenticate users. Trust they are who they say they are without verification.</li>
</ul>
</p>

<p><strong>Default:</strong> <code>None</code></p></td>
</tr>
<tr>
<td><code>balancerdExternalCertificateSpec</code></td>
<td></td>
<td>
<em><strong><a href='#materializecertspec'>MaterializeCertSpec</a></strong></em>


<p>The configuration for generating an x509 certificate using cert-manager for balancerd
to present to incoming connections.
The <code>dnsNames</code> and <code>issuerRef</code> fields are required.</p>

</td>
</tr>
<tr>
<td><code>balancerdReplicas</code></td>
<td></td>
<td>
<em><strong>Integer</strong></em>


<p>Number of balancerd pods to create.</p>

</td>
</tr>
<tr>
<td><code>balancerdResourceRequirements</code></td>
<td></td>
<td>
<em><strong><a href='#iok8sapicorev1resourcerequirements'>io.k8s.api.core.v1.ResourceRequirements</a></strong></em>


<p>Resource requirements for the balancerd pod.</p>

</td>
</tr>
<tr>
<td><code>consoleExternalCertificateSpec</code></td>
<td></td>
<td>
<em><strong><a href='#materializecertspec'>MaterializeCertSpec</a></strong></em>


<p>The configuration for generating an x509 certificate using cert-manager for the console
to present to incoming connections.
The <code>dnsNames</code> and <code>issuerRef</code> fields are required.
Not yet implemented.</p>

</td>
</tr>
<tr>
<td><code>consoleReplicas</code></td>
<td></td>
<td>
<em><strong>Integer</strong></em>


<p>Number of console pods to create.</p>

</td>
</tr>
<tr>
<td><code>consoleResourceRequirements</code></td>
<td></td>
<td>
<em><strong><a href='#iok8sapicorev1resourcerequirements'>io.k8s.api.core.v1.ResourceRequirements</a></strong></em>


<p>Resource requirements for the console pod.</p>

</td>
</tr>
<tr>
<td><code>enableRbac</code></td>
<td></td>
<td>
<em><strong>Bool</strong></em>


<p>Whether to enable role based access control. Defaults to false.</p>

</td>
</tr>
<tr>
<td><code>environmentId</code></td>
<td></td>
<td>
<em><strong>Uuid</strong></em>


<p>The value used by environmentd (via the &ndash;environment-id flag) to
uniquely identify this instance. Must be globally unique, and
is required if a license key is not provided.
NOTE: This value MUST NOT be changed in an existing instance,
since it affects things like the way data is stored in the persist
backend.</p>

<p><strong>Default:</strong> <code>00000000-0000-0000-0000-000000000000</code></p></td>
</tr>
<tr>
<td><code>environmentdConnectionRoleArn</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>If running in AWS, override the IAM role to use to support
the CREATE CONNECTION feature.</p>

</td>
</tr>
<tr>
<td><code>environmentdExtraArgs</code></td>
<td></td>
<td>
<em><strong>Array&lt;String&gt;</strong></em>


<p>Extra args to pass to the environmentd binary.</p>

</td>
</tr>
<tr>
<td><code>environmentdExtraEnv</code></td>
<td></td>
<td>
<em><strong>Array&lt;<a href='#iok8sapicorev1envvar'>io.k8s.api.core.v1.EnvVar</a>&gt;</strong></em>


<p>Extra environment variables to pass to the environmentd binary.</p>

</td>
</tr>
<tr>
<td><code>environmentdResourceRequirements</code></td>
<td></td>
<td>
<em><strong><a href='#iok8sapicorev1resourcerequirements'>io.k8s.api.core.v1.ResourceRequirements</a></strong></em>


<p>Resource requirements for the environmentd pod.</p>

</td>
</tr>
<tr>
<td><code>environmentdScratchVolumeStorageRequirement</code></td>
<td></td>
<td>
<em><strong>io.k8s.apimachinery.pkg.api.resource.Quantity</strong></em>


<p>Amount of disk to allocate, if a storage class is provided.</p>

</td>
</tr>
<tr>
<td><code>forcePromote</code></td>
<td></td>
<td>
<em><strong>Uuid</strong></em>


<p>If <code>forcePromote</code> is set to the same value as <code>requestRollout</code>, the
current rollout will skip waiting for clusters in the new
generation to rehydrate before promoting the new environmentd to
leader.</p>

<p><strong>Default:</strong> <code>00000000-0000-0000-0000-000000000000</code></p></td>
</tr>
<tr>
<td><code>forceRollout</code></td>
<td></td>
<td>
<em><strong>Uuid</strong></em>


<p>This value will be written to an annotation in the generated
environmentd statefulset, in order to force the controller to
detect the generated resources as changed even if no other changes
happened. This can be used to force a rollout to a new generation
even without making any meaningful changes, by setting it to the
same value as <code>requestRollout</code>.</p>

<p><strong>Default:</strong> <code>00000000-0000-0000-0000-000000000000</code></p></td>
</tr>
<tr>
<td><code>internalCertificateSpec</code></td>
<td></td>
<td>
<em><strong><a href='#materializecertspec'>MaterializeCertSpec</a></strong></em>


<p>The cert-manager Issuer or ClusterIssuer to use for database internal communication.
The <code>issuerRef</code> field is required.
This currently is only used for environmentd, but will eventually support clusterd.
Not yet implemented.</p>

</td>
</tr>
<tr>
<td><code>podAnnotations</code></td>
<td></td>
<td>
<em><strong>Map&lt;String, String&gt;</strong></em>


<p>Annotations to apply to the pods.</p>

</td>
</tr>
<tr>
<td><code>podLabels</code></td>
<td></td>
<td>
<em><strong>Map&lt;String, String&gt;</strong></em>


<p>Labels to apply to the pods.</p>

</td>
</tr>
<tr>
<td><code>requestRollout</code></td>
<td></td>
<td>
<em><strong>Uuid</strong></em>


<p><p>When changes are made to the environmentd resources (either via
modifying fields in the spec here or by deploying a new
orchestratord version which changes how resources are generated),
existing environmentd processes won&rsquo;t be automatically restarted.
In order to trigger a restart, the request_rollout field should be
set to a new (random) value. Once the rollout completes, the value
of <code>status.lastCompletedRolloutRequest</code> will be set to this value
to indicate completion.</p>
<p>Defaults to a random value in order to ensure that the first
generation rollout is automatically triggered.</p>
</p>

<p><strong>Default:</strong> <code>00000000-0000-0000-0000-000000000000</code></p></td>
</tr>
<tr>
<td><code>rolloutStrategy</code></td>
<td></td>
<td>
<em><strong>Enum</strong></em>


<p><p>Rollout strategy to use when upgrading this Materialize instance.</p>
<p>Valid values:</p>
<ul>
<li>
<p><code>WaitUntilReady</code> (default):<br>  Create a new generation of pods, leaving the old generation around until the
new ones are ready to take over.
This minimizes downtime, and is what almost everyone should use.</p>
</li>
<li>
<p><code>ManuallyPromote</code>:<br>  Create a new generation of pods, leaving the old generation as the serving generation
until the user manually promotes the new generation.</p>
<p>When using <code>ManuallyPromote</code>, the new generation can be promoted at any
time, even if it has dataflows that are not fully caught up, by setting
<code>forcePromote</code> to the same value as <code>requestRollout</code> in the Materialize spec.</p>
<p>To minimize downtime, promotion should occur when the new generation
has caught up to the prior generation. To determine if the new
generation has caught up, consult the <code>UpToDate</code> condition in the
status of the Materialize Resource. If the condition&rsquo;s reason is
<code>ReadyToPromote</code> the new generation is ready to promote.</p>
> **Warning:** Do not leave new generations unpromoted indefinitely.
>   The new generation keeps open read holds which prevent compaction. Once promoted or
>   cancelled, those read holds are released. If left unpromoted for an extended time, this
>   data can build up, and can cause extreme deletion load on the metadata backend database
>   when finally promoted or cancelled.

</li>
<li>
<p><code>ImmediatelyPromoteCausingDowntime</code>:<br>  > **Warning:** THIS WILL CAUSE YOUR MATERIALIZE INSTANCE TO BE UNAVAILABLE FOR SOME TIME!!!
>   This strategy should ONLY be used by customers with physical hardware who do not have
>   enough hardware for the `WaitUntilReady` strategy. If you think you want this, please
>   consult with Materialize engineering to discuss your situation.
</p>
<p>Tear down the old generation of pods and promote the new generation of pods immediately,
without waiting for the new generation of pods to be ready.</p>
</li>
</ul>
</p>

<p><strong>Default:</strong> <code>WaitUntilReady</code></p></td>
</tr>
<tr>
<td><code>serviceAccountAnnotations</code></td>
<td></td>
<td>
<em><strong>Map&lt;String, String&gt;</strong></em>


<p><p>Annotations to apply to the service account.</p>
<p>Annotations on service accounts are commonly used by cloud providers for IAM.
AWS uses &ldquo;eks.amazonaws.com/role-arn&rdquo;.
Azure uses &ldquo;azure.workload.identity/client-id&rdquo;, but
additionally requires &ldquo;azure.workload.identity/use&rdquo;: &ldquo;true&rdquo; on the pods.</p>
</p>

</td>
</tr>
<tr>
<td><code>serviceAccountLabels</code></td>
<td></td>
<td>
<em><strong>Map&lt;String, String&gt;</strong></em>


<p>Labels to apply to the service account.</p>

</td>
</tr>
<tr>
<td><code>serviceAccountName</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Name of the kubernetes service account to use.
If not set, we will create one with the same name as this Materialize object.</p>

</td>
</tr>
<tr>
<td><code>systemParameterConfigmapName</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p><p>The name of a ConfigMap containing system parameters in JSON format.
The ConfigMap must contain a <code>system-params.json</code> key whose value
is a valid JSON object containing valid system parameters.</p>
<p>Run <code>SHOW ALL</code> in SQL to see a subset of configurable system parameters.</p>
<p>Example ConfigMap:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-yaml" data-lang="yaml"><span class="line"><span class="cl"><span class="nt">data</span><span class="p">:</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w">  </span><span class="nt">system-params.json</span><span class="p">:</span><span class="w"> </span><span class="p">|</span><span class="sd">
</span></span></span><span class="line"><span class="cl"><span class="sd">    {
</span></span></span><span class="line"><span class="cl"><span class="sd">      &#34;max_connections&#34;: 1000
</span></span></span><span class="line"><span class="cl"><span class="sd">    }</span><span class="w">
</span></span></span></code></pre></div></p>

</td>
</tr>
</tbody>
</table>

#### MaterializeCertSpec
<table>
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
<td>
<em><strong>Array&lt;String&gt;</strong></em>


<p>Additional DNS names the certificate will be valid for.</p>

</td>
</tr>
<tr>
<td><code>duration</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Duration the certificate will be requested for.
Value must be in units accepted by Go
<a href="https://golang.org/pkg/time/#ParseDuration" ><code>time.ParseDuration</code></a>.</p>

</td>
</tr>
<tr>
<td><code>issuerRef</code></td>
<td></td>
<td>
<em><strong><a href='#certificateissuerref'>CertificateIssuerRef</a></strong></em>


<p>Reference to an <code>Issuer</code> or <code>ClusterIssuer</code> that will generate the certificate.</p>

</td>
</tr>
<tr>
<td><code>renewBefore</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Duration before expiration the certificate will be renewed.
Value must be in units accepted by Go
<a href="https://golang.org/pkg/time/#ParseDuration" ><code>time.ParseDuration</code></a>.</p>

</td>
</tr>
<tr>
<td><code>secretTemplate</code></td>
<td></td>
<td>
<em><strong><a href='#certificatesecrettemplate'>CertificateSecretTemplate</a></strong></em>


<p>Additional annotations and labels to include in the Certificate object.</p>

</td>
</tr>
</tbody>
</table>

#### CertificateSecretTemplate
<table>
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
<td>
<em><strong>Map&lt;String, String&gt;</strong></em>


<p>Annotations is a key value map to be copied to the target Kubernetes Secret.</p>

</td>
</tr>
<tr>
<td><code>labels</code></td>
<td></td>
<td>
<em><strong>Map&lt;String, String&gt;</strong></em>


<p>Labels is a key value map to be copied to the target Kubernetes Secret.</p>

</td>
</tr>
</tbody>
</table>

#### CertificateIssuerRef
<table>
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
<td>
<em><strong>String</strong></em>


<p>Name of the resource being referred to.</p>

</td>
</tr>
<tr>
<td><code>group</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Group of the resource being referred to.</p>

</td>
</tr>
<tr>
<td><code>kind</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Kind of the resource being referred to.</p>

</td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.ResourceRequirements
<table>
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
<td>
<em><strong>Array&lt;<a href='#iok8sapicorev1resourceclaim'>io.k8s.api.core.v1.ResourceClaim</a>&gt;</strong></em>


<p><p>Claims lists the names of resources, defined in spec.resourceClaims, that are used by this container.</p>
<p>This is an alpha field and requires enabling the DynamicResourceAllocation feature gate.</p>
<p>This field is immutable. It can only be set for containers.</p>
</p>

</td>
</tr>
<tr>
<td><code>limits</code></td>
<td></td>
<td>
<em><strong>Map&lt;String, io.k8s.apimachinery.pkg.api.resource.Quantity&gt;</strong></em>


<p>Limits describes the maximum amount of compute resources allowed. More info: <a href="https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/" >https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/</a></p>

</td>
</tr>
<tr>
<td><code>requests</code></td>
<td></td>
<td>
<em><strong>Map&lt;String, io.k8s.apimachinery.pkg.api.resource.Quantity&gt;</strong></em>


<p>Requests describes the minimum amount of compute resources required. If Requests is omitted for a container, it defaults to Limits if that is explicitly specified, otherwise to an implementation-defined value. Requests cannot exceed Limits. More info: <a href="https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/" >https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/</a></p>

</td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.ResourceClaim
<table>
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
<td>
<em><strong>String</strong></em>


<p>Name must match the name of one entry in pod.spec.resourceClaims of the Pod where this field is used. It makes that resource available inside a container.</p>

</td>
</tr>
<tr>
<td><code>request</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Request is the name chosen for a request in the referenced claim. If empty, everything from the claim is made available, otherwise only the result of this request.</p>

</td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.EnvVar
<table>
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
<td>
<em><strong>String</strong></em>


<p>Name of the environment variable. Must be a C_IDENTIFIER.</p>

</td>
</tr>
<tr>
<td><code>value</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Variable references $(VAR_NAME) are expanded using the previously defined environment variables in the container and any service environment variables. If a variable cannot be resolved, the reference in the input string will be unchanged. Double $$ are reduced to a single $, which allows for escaping the $(VAR_NAME) syntax: i.e. &ldquo;$$(VAR_NAME)&rdquo; will produce the string literal &ldquo;$(VAR_NAME)&rdquo;. Escaped references will never be expanded, regardless of whether the variable exists or not. Defaults to &ldquo;&rdquo;.</p>

</td>
</tr>
<tr>
<td><code>valueFrom</code></td>
<td></td>
<td>
<em><strong><a href='#iok8sapicorev1envvarsource'>io.k8s.api.core.v1.EnvVarSource</a></strong></em>


<p>Source for the environment variable&rsquo;s value. Cannot be used if value is not empty.</p>

</td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.EnvVarSource
<table>
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
<td>
<em><strong><a href='#iok8sapicorev1configmapkeyselector'>io.k8s.api.core.v1.ConfigMapKeySelector</a></strong></em>


<p>Selects a key of a ConfigMap.</p>

</td>
</tr>
<tr>
<td><code>fieldRef</code></td>
<td></td>
<td>
<em><strong><a href='#iok8sapicorev1objectfieldselector'>io.k8s.api.core.v1.ObjectFieldSelector</a></strong></em>


<p>Selects a field of the pod: supports metadata.name, metadata.namespace, <code>metadata.labels['&lt;KEY&gt;']</code>, <code>metadata.annotations['&lt;KEY&gt;']</code>, spec.nodeName, spec.serviceAccountName, status.hostIP, status.podIP, status.podIPs.</p>

</td>
</tr>
<tr>
<td><code>resourceFieldRef</code></td>
<td></td>
<td>
<em><strong><a href='#iok8sapicorev1resourcefieldselector'>io.k8s.api.core.v1.ResourceFieldSelector</a></strong></em>


<p>Selects a resource of the container: only resources limits and requests (limits.cpu, limits.memory, limits.ephemeral-storage, requests.cpu, requests.memory and requests.ephemeral-storage) are currently supported.</p>

</td>
</tr>
<tr>
<td><code>secretKeyRef</code></td>
<td></td>
<td>
<em><strong><a href='#iok8sapicorev1secretkeyselector'>io.k8s.api.core.v1.SecretKeySelector</a></strong></em>


<p>Selects a key of a secret in the pod&rsquo;s namespace</p>

</td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.SecretKeySelector
<table>
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
<td>
<em><strong>String</strong></em>


<p>The key of the secret to select from.  Must be a valid secret key.</p>

</td>
</tr>
<tr>
<td><code>name</code></td>
<td>✅</td>
<td>
<em><strong>String</strong></em>


<p>Name of the referent. This field is effectively required, but due to backwards compatibility is allowed to be empty. Instances of this type with an empty value here are almost certainly wrong. More info: <a href="https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names" >https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names</a></p>

</td>
</tr>
<tr>
<td><code>optional</code></td>
<td></td>
<td>
<em><strong>Bool</strong></em>


<p>Specify whether the Secret or its key must be defined</p>

</td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.ResourceFieldSelector
<table>
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
<td>
<em><strong>String</strong></em>


<p>Required: resource to select</p>

</td>
</tr>
<tr>
<td><code>containerName</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Container name: required for volumes, optional for env vars</p>

</td>
</tr>
<tr>
<td><code>divisor</code></td>
<td></td>
<td>
<em><strong>io.k8s.apimachinery.pkg.api.resource.Quantity</strong></em>


<p>Specifies the output format of the exposed resources, defaults to &ldquo;1&rdquo;</p>

</td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.ObjectFieldSelector
<table>
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
<td>
<em><strong>String</strong></em>


<p>Path of the field to select in the specified API version.</p>

</td>
</tr>
<tr>
<td><code>apiVersion</code></td>
<td></td>
<td>
<em><strong>String</strong></em>


<p>Version of the schema the FieldPath is written in terms of, defaults to &ldquo;v1&rdquo;.</p>

</td>
</tr>
</tbody>
</table>

#### io.k8s.api.core.v1.ConfigMapKeySelector
<table>
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
<td>
<em><strong>String</strong></em>


<p>The key to select.</p>

</td>
</tr>
<tr>
<td><code>name</code></td>
<td>✅</td>
<td>
<em><strong>String</strong></em>


<p>Name of the referent. This field is effectively required, but due to backwards compatibility is allowed to be empty. Instances of this type with an empty value here are almost certainly wrong. More info: <a href="https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names" >https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names</a></p>

</td>
</tr>
<tr>
<td><code>optional</code></td>
<td></td>
<td>
<em><strong>Bool</strong></em>


<p>Specify whether the ConfigMap or its key must be defined</p>

</td>
</tr>
</tbody>
</table>
