# Upgrading

Upgrading Self-Managed Materialize.



Materialize releases new Self-Managed versions per the schedule outlined in [Release schedule](/releases/schedule/#self-managed-release-schedule).

## General rules for upgrading

<p>When upgrading:</p>
<ul>
<li>
<p><strong>Always</strong> check the <a href="/self-managed-deployments/upgrading/#version-specific-upgrade-notes" >version specific upgrade
notes</a>.</p>
</li>
<li>
<strong>Always</strong> upgrade the Materialize Operator <strong>before</strong>
upgrading the Materialize instances.
</li>
</ul>


> **Note:** For major version upgrades, you can <strong>only</strong> upgrade <strong>one</strong> major version
> at a time. For example, upgrades from <strong>v26</strong>.1.0 to <strong>v27</strong>.3.0 is
> permitted but <strong>v26</strong>.1.0 to <strong>v28</strong>.0.0 is not.



## Upgrade guides

The following upgrade guides are available as examples:

<h4 id="upgrade-using-helm-commands">Upgrade using Helm Commands</h4>
<table>
  <thead>
      <tr>
          <th>Guide</th>
          <th>Description</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td><a href="/self-managed-deployments/upgrading/upgrade-on-kind/" >Upgrade on Kind</a></td>
          <td>Uses standard Helm commands to upgrade Materialize on a Kind cluster in Docker.</td>
      </tr>
  </tbody>
</table>


<h4 id="upgrade-using-the-new-terraform-modules">Upgrade using the new Terraform Modules</h4>
> **Tip:** The Terraform modules are provided as examples. They are not required for
> upgrading Materialize.

<table>
  <thead>
      <tr>
          <th>Guide</th>
          <th>Description</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td><a href="/self-managed-deployments/upgrading/upgrade-on-aws/" >Upgrade on AWS (Terraform)</a></td>
          <td>Uses the new Terraform module to deploy Materialize to AWS Elastic Kubernetes Service (EKS).</td>
      </tr>
      <tr>
          <td><a href="/self-managed-deployments/upgrading/upgrade-on-azure/" >Upgrade on Azure (Terraform)</a></td>
          <td>Uses the new Terraform module to deploy Materialize to Azure Kubernetes Service (AKS).</td>
      </tr>
      <tr>
          <td><a href="/self-managed-deployments/upgrading/upgrade-on-gcp/" >Upgrade on GCP (Terraform)</a></td>
          <td>Uses the new Terraform module to deploy Materialize to Google Kubernetes Engine (GKE).</td>
      </tr>
  </tbody>
</table>


<h4 id="upgrade-using-legacy-terraform-modules">Upgrade using Legacy Terraform Modules</h4>
> **Tip:** The Terraform modules are provided as examples. They are not required for
> upgrading Materialize.

<table>
  <thead>
      <tr>
          <th>Guide</th>
          <th>Description</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td><a href="/self-managed-deployments/upgrading/legacy/upgrade-on-aws-legacy/" >Upgrade on AWS (Legacy Terraform)</a></td>
          <td>Uses legacy Terraform module to deploy Materialize to AWS Elastic Kubernetes Service (EKS).</td>
      </tr>
      <tr>
          <td><a href="/self-managed-deployments/upgrading/legacy/upgrade-on-azure-legacy/" >Upgrade on Azure (Legacy Terraform)</a></td>
          <td>Uses legacy Terraform module to deploy Materialize to Azure Kubernetes Service (AKS).</td>
      </tr>
      <tr>
          <td><a href="/self-managed-deployments/upgrading/legacy/upgrade-on-gcp-legacy/" >Upgrade on GCP (Legacy Terraform)</a></td>
          <td>Uses legacy Terraform module to deploy Materialize to Google Kubernetes Engine (GKE).</td>
      </tr>
  </tbody>
</table>


## Upgrading the Helm Chart and Materialize Operator

> **Important:** When upgrading Materialize, always upgrade the Helm Chart and Materialize
> Operator first.


### Update the Helm Chart repository

To update your Materialize Helm Chart repository:

```shell
helm repo update materialize
```

View the available chart versions:

```shell
helm search repo materialize/materialize-operator --versions
```

### Upgrade your Materialize Operator

The Materialize Kubernetes Operator is deployed via Helm and can be updated
through standard `helm upgrade` command:



```mzsql
helm upgrade -n <namespace> <release-name> materialize/materialize-operator \
  --version <new_version> \
  -f <your-custom-values.yml>

```

| Syntax element | Description |
| --- | --- |
| `<namespace>` | The namespace where the Operator is running. (e.g., `materialize`)  |
| `<release-name>` | The release name. You can use `helm list -n <namespace>` to find your release name.  |
| `<new_version>` | The upgrade version.  |
| `<your-custom-values.yml>` | The name of your customization file, if using. If you are configuring using `\-\-set key=value` options, include them as well.  |


You can use `helm list` to find your release name. For example, if your Operator
is running in the namespace `materialize`, run `helm list`:

```shell
helm list -n materialize
```

Retrieve the name associated with the `materialize-operator` **CHART**; for
example, `my-demo` in the following helm list:

```none
NAME    	  NAMESPACE  	REVISION	UPDATED                             	STATUS  	CHART                                          APP VERSION
my-demo	materialize	1      2025-12-08 11:39:50.185976 -0500 EST	deployed	materialize-operator-v26.1.0    v26.1.0
```

Then, to upgrade:

```shell
helm upgrade -n materialize my-demo materialize/operator \
  -f my-values.yaml \
  --version v26.8.0
```

## Upgrading Materialize Instances

**After** you have upgraded your Materialize Operator, upgrade your Materialize
instance(s) to the **APP Version** of the Operator. To find the version of your
currently deployed Materialize Operator:

```shell
helm list -n materialize
```

You will use the returned **App Version** for the updated `environmentdImageRef`
value. Specifically, for your Materialize instance(s), set
`environmentdImageRef` value to use the new version:

```
spec:
  environmentdImageRef: docker.io/materialize/environmentd:<app_version>
```

To minimize unexpected downtime and avoid connection drops at critical
periods for your application, the upgrade process involves two steps:

- First, stage the changes (update the `environmentdImageRef` with the new
  version) to the Materialize custom resource. The Operator watches for changes
  but does not automatically roll out the changes.

- Second, roll out the changes by specifying a new UUID for `requestRollout`.

### Stage the Materialize instance version change

To stage the Materialize instances version upgrade, update the
`environmentdImageRef` field in the Materialize custom resource spec to the
compatible version of your currently deployed Materialize Operator.

To stage, but **not** rollout, the Materialize instance version upgrade, you can
use the `kubectl patch` command; for example, if the **App Version** is v26.8.0:

```shell
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"docker.io/materialize/environmentd:v26.8.0\"}}"
```

> **Note:** Until you specify a new `requestRollout`, the Operator watches for updates but
> does not roll out the changes.



### Applying the changes via `requestRollout`

To apply chang Materialize instance upgrade, you must update the `requestRollout` field in the Materialize custom resource spec to a new UUID.
Be sure to consult the [Rollout Configurations](#rollout-configuration) to ensure you've selected the correct rollout behavior.
```shell
# Then trigger the rollout with a new UUID
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\"}}"
```

### Staging and applying in a single command

Although separating the staging and rollout of the changes into two steps can
minimize unexpected downtime and avoid connection drops at critical periods, you
can, if preferred, combine both operations in a single command

```shell
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"environmentdImageRef\": \"docker.io/materialize/environmentd:v26.8.0\", \"requestRollout\": \"$(uuidgen)\"}}"
```

#### Using YAML Definition

Alternatively, you can update your Materialize custom resource definition directly:

```yaml
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:v26.8.0 # Update version as needed
  requestRollout: 22222222-2222-2222-2222-222222222222    # Use a new UUID
  forceRollout: 33333333-3333-3333-3333-333333333333      # Optional: for forced rollouts
  inPlaceRollout: false                                   # In Place rollout is deprecated and ignored. Please use rolloutStrategy
  rolloutStrategy: WaitUntilReady                         # The mechanism to use when rolling out the new version. Can be WaitUntilReady or ImmediatelyPromoteCausingDowntime
  backendSecretName: materialize-backend
```

Apply the updated definition:

```shell
kubectl apply -f materialize.yaml
```

## Rollout Configuration

### `requestRollout`

Specify a new `UUID` value for the `requestRollout` to roll out the changes to
the Materialize instance.

> **Note:** `requestRollout` without the `forcedRollout` field only rolls out if changes
> exist to the Materialize instance. To roll out even if there are no changes to
> the instance, use with `forcedRollouts`.


```shell
# Only rolls out if there are changes
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\"}}"
```
#### `requestRollout` with `forcedRollouts`

Specify a new `UUID` value for `forcedRollout` to roll out even when there are
no changes to the instance. Use `forcedRollout` with `requestRollout`.

```shell
kubectl patch materialize <instance-name> \
  -n materialize-environment \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\", \"forceRollout\": \"$(uuidgen)\"}}"
```

### Rollout strategies

Rollout strategies control how Materialize transitions from the current generation to a new generation during an upgrade.

The behavior of the new version rollout follows your `rolloutStrategy` setting.

#### *WaitUntilReady* - ***Default***

`WaitUntilReady` creates a new generation of pods and automatically cuts over to them as soon as they catch up to the old generation and become `ReadyToPromote`. This strategy temporarily doubles the required resources to run Materialize.
> **Warning:** `WaitUntilReady` waits up to 72 hours (configurable by the `with_0dt_deployment_max_wait` flag) for the new pods to become ready. If the promotion has not occurred by then, the new pods are automatically promoted.


#### *ImmediatelyPromoteCausingDowntime*
> **Warning:** Using the `ImmediatelyPromoteCausingDowntime` rollout flag will cause downtime.


`ImmediatelyPromoteCausingDowntime` tears down the prior generation, and immediately promotes the new generation without waiting for it to hydrate. This causes downtime until the new generation has hydrated. However, it does not require additional resources.

#### *ManuallyPromote*

`ManuallyPromote` allows you to choose when to promote the new generation. This means you can time the promotion for periods when load is low, minimizing the impact of potential downtime for any clients connected to Materialize. This strategy temporarily doubles the required resources to run Materialize.

To minimize downtime, wait until the new generation has fully hydrated and caught up to the prior generation before promoting. To check hydration status, inspect the `UpToDate` condition in the Materialize resource status. When hydration completes, the condition will be `ReadyToPromote`.

To promote, update the `forcePromote` field to match the `requestRollout` field in the Materialize spec. If you need to promote before hydration completes, you can set `forcePromote` immediately, but clients may experience downtime.

> **Warning:** Leaving a new generation unpromoted for over 6 hours may cause downtime.


**Do not leave new generations unpromoted indefinitely**. They should either be promoted or canceled. New generations open a read hold on the metadata database that prevents compaction. This hold is only released when the generation is promoted or canceled. If left open too long, promoting or canceling can trigger a spike in deletion load on the metadata database, potentially causing downtime. It is not recommended to leave generations unpromoted for over 6 hours.

#### *inPlaceRollout* - ***Deprecated***

The setting is ignored.

## Verifying the Upgrade

After initiating the rollout, you can monitor the status field of the Materialize custom resource to check on the upgrade.

```shell
# Watch the status of your Materialize environment
kubectl get materialize -n materialize-environment -w

# Check the logs of the operator
kubectl logs -l app.kubernetes.io/name=materialize-operator -n materialize
```

## Version Specific Upgrade Notes

### Upgrading to `v26.1` and later versions

<ul>
<li>To upgrade to <code>v26.1</code> or future versions, you must first upgrade to <code>v26.0</code></li>
</ul>


### Upgrading to `v26.0`

<ul>
<li>
<p>Upgrading to <code>v26.0.0</code> is a major version upgrade. To upgrade to <code>v26.0</code> from
<code>v25.2.X</code> or <code>v25.1</code>, you must first upgrade to <code>v25.2.16</code> and then upgrade to
<code>v26.0.0</code>.</p>
</li>
<li>
<p>For upgrades, the <code>inPlaceRollout</code> setting has been deprecated and will be
ignored. Instead, use the new setting <code>rolloutStrategy</code> to specify either:</p>
<ul>
<li><code>WaitUntilReady</code> (<em>Default</em>)</li>
<li><code>ImmediatelyPromoteCausingDowntime</code></li>
</ul>
<p>For more information, see
<a href="/self-managed-deployments/upgrading/#rollout-strategies" ><code>rolloutStrategy</code></a>.</p>
</li>
<li>
<p>New requirements were introduced for <a href="/releases/#license-key" >license keys</a>.
To upgrade, you will first need to add a license key to the <code>backendSecret</code>
used in the spec for your Materialize resource.</p>
<p>See <a href="/releases/#license-key" >License key</a> for details on getting your license
key.</p>
</li>
<li>
<p>Swap is now enabled by default. Swap reduces the memory required to
operate Materialize and improves cost efficiency. Upgrading to <code>v26.0</code>
requires some preparation to ensure Kubernetes nodes are labeled
and configured correctly. As such:</p>
<ul>
<li>
<p>If you are using the Materialize-provided Terraforms, upgrade to version
<code>v0.6.1</code> of the Terraform.</p>
</li>
<li>
<p>If you are <red><strong>not</strong></red> using a Materialize-provided Terraform, refer
to <a href="/self-managed-deployments/appendix/upgrade-to-swap/" >Prepare for swap and upgrade to v26.0</a>.</p>
</li>
</ul>
</li>
</ul>


### Upgrading between minor versions less than `v26`
 - Prior to `v26`, you must upgrade at most one minor version at a time. For
   example, upgrading from `v25.1.5` to `v25.2.16` is permitted.

## See also

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)

- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)

- [Troubleshooting](/self-managed-deployments/troubleshooting/)



---

## Upgrade Guides (Legacy)


<h4 id="upgrade-using-legacy-terraform-modules">Upgrade using Legacy Terraform Modules</h4>
> **Tip:** The Terraform modules are provided as examples. They are not required for
> upgrading Materialize.

<table>
  <thead>
      <tr>
          <th>Guide</th>
          <th>Description</th>
      </tr>
  </thead>
  <tbody>
      <tr>
          <td><a href="/self-managed-deployments/upgrading/legacy/upgrade-on-aws-legacy/" >Upgrade on AWS (Legacy Terraform)</a></td>
          <td>Uses legacy Terraform module to deploy Materialize to AWS Elastic Kubernetes Service (EKS).</td>
      </tr>
      <tr>
          <td><a href="/self-managed-deployments/upgrading/legacy/upgrade-on-azure-legacy/" >Upgrade on Azure (Legacy Terraform)</a></td>
          <td>Uses legacy Terraform module to deploy Materialize to Azure Kubernetes Service (AKS).</td>
      </tr>
      <tr>
          <td><a href="/self-managed-deployments/upgrading/legacy/upgrade-on-gcp-legacy/" >Upgrade on GCP (Legacy Terraform)</a></td>
          <td>Uses legacy Terraform module to deploy Materialize to Google Kubernetes Engine (GKE).</td>
      </tr>
  </tbody>
</table>



---

## Upgrade on AWS


The following tutorial upgrades your Materialize deployment running on AWS
Elastic Kubernetes Service (EKS). The tutorial assumes you have installed the
example on [Install on
AWS](/self-managed-deployments/installation/install-on-aws/).

## Upgrade guidelines

<p>When upgrading:</p>
<ul>
<li>
<p><strong>Always</strong> check the <a href="/self-managed-deployments/upgrading/#version-specific-upgrade-notes" >version specific upgrade
notes</a>.</p>
</li>
<li>
<strong>Always</strong> upgrade the Materialize Operator <strong>before</strong>
upgrading the Materialize instances.
</li>
</ul>


> **Note:** For major version upgrades, you can <strong>only</strong> upgrade <strong>one</strong> major version
> at a time. For example, upgrades from <strong>v26</strong>.1.0 to <strong>v27</strong>.3.0 is
> permitted but <strong>v26</strong>.1.0 to <strong>v28</strong>.0.0 is not.


> **Note:** Downgrading is not supported.


## Prerequisites

### Required Tools

- [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
- [kubectl](https://docs.aws.amazon.com/eks/latest/userguide/install-kubectl.html)
- [Helm 3.2.0+](https://helm.sh/docs/intro/install/)

## Upgrade process

> **Important:** The following procedure performs a rolling upgrade, where both the old and new Materialize instances are running before the old instances are removed. When performing a rolling upgrade, ensure you have enough resources to support having both the old and new Materialize instances running.


### Step 1: Set up

1. Open a Terminal window.

1. Configure AWS CLI with your AWS credentials. For details, see the [AWS
   documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).

1. Go to the Terraform directory for your Materialize deployment. For example,
   if you deployed from the `aws/examples/simple` directory:

   ```bash
   cd materialize-terraform-self-managed/aws/examples/simple
   ```

1. Ensure your AWS CLI is configured with the appropriate profile, substitute
   `<your-aws-profile>` with the profile to use:

   ```bash
   # Set your AWS profile for the session
   export AWS_PROFILE=<your-aws-profile>
   ```

1. Configure `kubectl` to connect to your EKS cluster, replacing:

   - `<your-eks-cluster-name>` with the your cluster name; i.e., the
     `eks_cluster_name` in the Terraform output. For the
     sample example, your cluster name has the form `{prefix_name}-eks`; e.g.,
     `simple-demo-eks`.

   - `<your-region>` with the region of your cluster. Your region can be
     found in your `terraform.tfvars` file; e.g., `us-east-1`.

   ```bash
   # aws eks update-kubeconfig --name <your-eks-cluster-name> --region <your-region>
   aws eks update-kubeconfig --name $(terraform output -raw eks_cluster_name) --region <your-region>
   ```

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl get nodes
   ```

   For help with `kubectl` commands, see [kubectl Quick reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

### Step 2: Update the Helm Chart

> **Important:** <strong>Always</strong> upgrade the Materialize Operator <strong>before</strong>
> upgrading the Materialize instances.


<p>To update your Materialize Helm Chart repository:</p>
<ol>
<li>
<p>Update the Helm repo:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm repo update materialize
</span></span></code></pre></div></li>
<li>
<p>View the available chart versions:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm search repo materialize/materialize-operator --versions
</span></span></code></pre></div></li>
</ol>


### Step 3: Upgrade the Materialize Operator

> **Important:** <strong>Always</strong> upgrade the Materialize Operator <strong>before</strong>
> upgrading the Materialize instances.


<ol>
<li>
<p>Use <code>helm list</code> to find the release name. The sample example
deployment using the unified Terraform module deploys the Operator in the
<code>materialize</code> namespace.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm list -n materialize
</span></span></code></pre></div><p>Retrieve the release name (corresponds to the <code>name_prefix</code> variable
specified in your <code>terraform.tfvars</code>) associated with the
<code>materialize-operator</code> <strong>CHART</strong>; for example, <code>simple-demo</code> in the following output:</p>
<pre tabindex="0"><code class="language-none" data-lang="none">NAME    	    NAMESPACE    REVISION   UPDATED                               STATUS     CHART                         APP VERSION
simple-demo  materialize  1         2025-12-08 11:39:50.185976 -0500 EST   deployed  materialize-operator-v26.1.0  v26.1.0
</code></pre></li>
<li>
<p>Export your current values to a file to preserve any custom settings:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm get values -n materialize simple-demo -o yaml &gt; my-values.yaml
</span></span></code></pre></div></li>
<li>
<p>Upgrade your Operator. For example, the following upgrades the Operator
to v26.8.0:</p>
> **Note:** For major version upgrades, you can <strong>only</strong> upgrade <strong>one</strong> major version
>    at a time. For example, upgrades from <strong>v26</strong>.1.0 to <strong>v27</strong>.3.0 is
>    permitted but <strong>v26</strong>.1.0 to <strong>v28</strong>.0.0 is not.

<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm upgrade -n materialize simple-demo materialize/materialize-operator  <span class="se">\
</span></span></span><span class="line"><span class="cl">  -f my-values.yaml <span class="se">\
</span></span></span><span class="line"><span class="cl">  --version v26.8.0
</span></span></code></pre></div></li>
<li>
<p>Verify that the Operator is running:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-bash" data-lang="bash"><span class="line"><span class="cl">kubectl -n materialize get all
</span></span></code></pre></div></li>
<li>
<p>Get the <strong>APP VERSION</strong> of the Operator.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm list -n materialize
</span></span></code></pre></div><p>The <strong>APP VERSION</strong> will be the value that you will use for upgrading
Materialize instances.</p>
</li>
</ol>


### Step 4: Upgrading Materialize Instances

> **Important:** <strong>Always</strong> upgrade the Materialize Operator <strong>before</strong>
> upgrading the Materialize instances.


<p><strong>After</strong> you have upgraded your Materialize Operator, upgrade your
Materialize instance(s) to the <strong>APP Version</strong> of the Operator. When
upgrading Materialize Instances versions, changes are not automatically
rolled out by the Operator in order to minimize unexpected downtime and
avoid connection drops at critical periods. Instead, the upgrade process
involves two steps:</p>
<ul>
<li>First, staging the version change to the Materialize custom resource.</li>
<li>Second, rolling out the changes via a <code>requestRollout</code> flag.</li>
</ul>
<ol>
<li>
<p>Find the name of the Materialize instance to upgrade. The sample example
deployment using the unified Terraform module deploys the Materialie
instance in the<code>materialize-environment</code> namespace.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">kubectl get materialize -n  materialize-environment
</span></span></code></pre></div><p>In the example deployment, the name of the instance is <code>main</code>.</p>
<pre tabindex="0"><code class="language-none" data-lang="none">NAME
main
</code></pre></li>
<li>
<p>Stage, but not rollout, the Materialize instance version upgrade.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">kubectl patch materialize main<span class="se">\
</span></span></span><span class="line"><span class="cl">  -n materialize-environment <span class="se">\
</span></span></span><span class="line"><span class="cl">  --type<span class="o">=</span><span class="s1">&#39;merge&#39;</span> <span class="se">\
</span></span></span><span class="line"><span class="cl">  -p <span class="s2">&#34;{\&#34;spec\&#34;: {\&#34;environmentdImageRef\&#34;: \&#34;docker.io/materialize/environmentd:v26.8.0\&#34;}}&#34;</span>
</span></span></code></pre></div></li>
<li>
<p>Rollout the Materialize instance version change.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">kubectl patch materialize main <span class="se">\
</span></span></span><span class="line"><span class="cl">  -n materialize-environment <span class="se">\
</span></span></span><span class="line"><span class="cl">  --type<span class="o">=</span><span class="s1">&#39;merge&#39;</span> <span class="se">\
</span></span></span><span class="line"><span class="cl">  -p <span class="s2">&#34;{\&#34;spec\&#34;: {\&#34;requestRollout\&#34;: \&#34;</span><span class="k">$(</span>uuidgen<span class="k">)</span><span class="s2">\&#34;}}&#34;</span>
</span></span></code></pre></div></li>
<li>
<p>Verify the upgrade by checking the <code>environmentd</code> events:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-bash" data-lang="bash"><span class="line"><span class="cl">kubectl -n materialize-environment describe pod -l <span class="nv">app</span><span class="o">=</span>environmentd
</span></span></code></pre></div></li>
</ol>


## See also

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)
- [Troubleshooting](/self-managed-deployments/troubleshooting/)


---

## Upgrade on Azure


The following tutorial upgrades your Materialize deployment running on Azure
Kubernetes Service (AKS). The tutorial assumes you have installed the
example on [Install on
Azure](/self-managed-deployments/installation/install-on-azure/).

## Upgrade guidelines

<p>When upgrading:</p>
<ul>
<li>
<p><strong>Always</strong> check the <a href="/self-managed-deployments/upgrading/#version-specific-upgrade-notes" >version specific upgrade
notes</a>.</p>
</li>
<li>
<strong>Always</strong> upgrade the Materialize Operator <strong>before</strong>
upgrading the Materialize instances.
</li>
</ul>


> **Note:** For major version upgrades, you can <strong>only</strong> upgrade <strong>one</strong> major version
> at a time. For example, upgrades from <strong>v26</strong>.1.0 to <strong>v27</strong>.3.0 is
> permitted but <strong>v26</strong>.1.0 to <strong>v28</strong>.0.0 is not.


> **Note:** Downgrading is not supported.


## Prerequisites

### Required Tools

- [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform)
- [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Helm 3.2.0+](https://helm.sh/docs/intro/install/)

## Upgrade process

> **Important:** The following procedure performs a rolling upgrade, where both the old and new Materialize instances are running before the old instances are removed. When performing a rolling upgrade, ensure you have enough resources to support having both the old and new Materialize instances running.


### Step 1: Set up

1. Open a Terminal window.

1. Configure Azure CLI with your Azure credentials. For details, see the [Azure
   documentation](https://learn.microsoft.com/en-us/cli/azure/authenticate-azure-cli).

1. Go to the Terraform directory for your Materialize deployment. For example,
   if you deployed from the `azure/examples/simple` directory:

   ```bash
   cd materialize-terraform-self-managed/azure/examples/simple
   ```

1. Configure `kubectl` to connect to your AKS cluster, replacing:

   - `<your-resource-group-name>` with your resource group name; i.e., the
     `resource_group_name` in the Terraform output or in the
     `terraform.tfvars` file.

   - `<your-aks-cluster-name>` with your cluster name; i.e., the
     `aks_cluster_name` in the Terraform output. For the sample example,
     your cluster name has the form `{prefix_name}-aks`; e.g., simple-demo-aks`.

   ```bash
   # az aks get-credentials --resource-group <your-resource-group-name> --name <your-aks-cluster-name>
   az aks get-credentials --resource-group $(terraform output -raw resource_group_name) --name $(terraform output -raw aks_cluster_name)
   ```

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl get nodes
   ```

   For help with `kubectl` commands, see [kubectl Quick reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

### Step 2: Update the Helm Chart

> **Important:** <strong>Always</strong> upgrade the Materialize Operator <strong>before</strong>
> upgrading the Materialize instances.


<p>To update your Materialize Helm Chart repository:</p>
<ol>
<li>
<p>Update the Helm repo:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm repo update materialize
</span></span></code></pre></div></li>
<li>
<p>View the available chart versions:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm search repo materialize/materialize-operator --versions
</span></span></code></pre></div></li>
</ol>


### Step 3: Upgrade the Materialize Operator

> **Important:** <strong>Always</strong> upgrade the Materialize Operator <strong>before</strong>
> upgrading the Materialize instances.


<ol>
<li>
<p>Use <code>helm list</code> to find the release name. The sample example
deployment using the unified Terraform module deploys the Operator in the
<code>materialize</code> namespace.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm list -n materialize
</span></span></code></pre></div><p>Retrieve the release name (corresponds to the <code>name_prefix</code> variable
specified in your <code>terraform.tfvars</code>) associated with the
<code>materialize-operator</code> <strong>CHART</strong>; for example, <code>simple-demo</code> in the following output:</p>
<pre tabindex="0"><code class="language-none" data-lang="none">NAME    	    NAMESPACE    REVISION   UPDATED                               STATUS     CHART                         APP VERSION
simple-demo  materialize  1         2025-12-08 11:39:50.185976 -0500 EST   deployed  materialize-operator-v26.1.0  v26.1.0
</code></pre></li>
<li>
<p>Export your current values to a file to preserve any custom settings:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm get values -n materialize simple-demo -o yaml &gt; my-values.yaml
</span></span></code></pre></div></li>
<li>
<p>Upgrade your Operator. For example, the following upgrades the Operator
to v26.8.0:</p>
> **Note:** For major version upgrades, you can <strong>only</strong> upgrade <strong>one</strong> major version
>    at a time. For example, upgrades from <strong>v26</strong>.1.0 to <strong>v27</strong>.3.0 is
>    permitted but <strong>v26</strong>.1.0 to <strong>v28</strong>.0.0 is not.

<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm upgrade -n materialize simple-demo materialize/materialize-operator  <span class="se">\
</span></span></span><span class="line"><span class="cl">  -f my-values.yaml <span class="se">\
</span></span></span><span class="line"><span class="cl">  --version v26.8.0
</span></span></code></pre></div></li>
<li>
<p>Verify that the Operator is running:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-bash" data-lang="bash"><span class="line"><span class="cl">kubectl -n materialize get all
</span></span></code></pre></div></li>
<li>
<p>Get the <strong>APP VERSION</strong> of the Operator.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm list -n materialize
</span></span></code></pre></div><p>The <strong>APP VERSION</strong> will be the value that you will use for upgrading
Materialize instances.</p>
</li>
</ol>


### Step 4: Upgrading Materialize Instances

> **Important:** <strong>Always</strong> upgrade the Materialize Operator <strong>before</strong>
> upgrading the Materialize instances.


<p><strong>After</strong> you have upgraded your Materialize Operator, upgrade your
Materialize instance(s) to the <strong>APP Version</strong> of the Operator. When
upgrading Materialize Instances versions, changes are not automatically
rolled out by the Operator in order to minimize unexpected downtime and
avoid connection drops at critical periods. Instead, the upgrade process
involves two steps:</p>
<ul>
<li>First, staging the version change to the Materialize custom resource.</li>
<li>Second, rolling out the changes via a <code>requestRollout</code> flag.</li>
</ul>
<ol>
<li>
<p>Find the name of the Materialize instance to upgrade. The sample example
deployment using the unified Terraform module deploys the Materialie
instance in the<code>materialize-environment</code> namespace.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">kubectl get materialize -n  materialize-environment
</span></span></code></pre></div><p>In the example deployment, the name of the instance is <code>main</code>.</p>
<pre tabindex="0"><code class="language-none" data-lang="none">NAME
main
</code></pre></li>
<li>
<p>Stage, but not rollout, the Materialize instance version upgrade.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">kubectl patch materialize main<span class="se">\
</span></span></span><span class="line"><span class="cl">  -n materialize-environment <span class="se">\
</span></span></span><span class="line"><span class="cl">  --type<span class="o">=</span><span class="s1">&#39;merge&#39;</span> <span class="se">\
</span></span></span><span class="line"><span class="cl">  -p <span class="s2">&#34;{\&#34;spec\&#34;: {\&#34;environmentdImageRef\&#34;: \&#34;docker.io/materialize/environmentd:v26.8.0\&#34;}}&#34;</span>
</span></span></code></pre></div></li>
<li>
<p>Rollout the Materialize instance version change.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">kubectl patch materialize main <span class="se">\
</span></span></span><span class="line"><span class="cl">  -n materialize-environment <span class="se">\
</span></span></span><span class="line"><span class="cl">  --type<span class="o">=</span><span class="s1">&#39;merge&#39;</span> <span class="se">\
</span></span></span><span class="line"><span class="cl">  -p <span class="s2">&#34;{\&#34;spec\&#34;: {\&#34;requestRollout\&#34;: \&#34;</span><span class="k">$(</span>uuidgen<span class="k">)</span><span class="s2">\&#34;}}&#34;</span>
</span></span></code></pre></div></li>
<li>
<p>Verify the upgrade by checking the <code>environmentd</code> events:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-bash" data-lang="bash"><span class="line"><span class="cl">kubectl -n materialize-environment describe pod -l <span class="nv">app</span><span class="o">=</span>environmentd
</span></span></code></pre></div></li>
</ol>


## See also

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)
- [Troubleshooting](/self-managed-deployments/troubleshooting/)


---

## Upgrade on GCP


The following tutorial upgrades your Materialize deployment running on Google
Kubernetes Engine (GKE). The tutorial assumes you have installed the
example on [Install on
GCP](/self-managed-deployments/installation/install-on-gcp/).

## Upgrade guidelines

<p>When upgrading:</p>
<ul>
<li>
<p><strong>Always</strong> check the <a href="/self-managed-deployments/upgrading/#version-specific-upgrade-notes" >version specific upgrade
notes</a>.</p>
</li>
<li>
<strong>Always</strong> upgrade the Materialize Operator <strong>before</strong>
upgrading the Materialize instances.
</li>
</ul>


> **Note:** For major version upgrades, you can <strong>only</strong> upgrade <strong>one</strong> major version
> at a time. For example, upgrades from <strong>v26</strong>.1.0 to <strong>v27</strong>.3.0 is
> permitted but <strong>v26</strong>.1.0 to <strong>v28</strong>.0.0 is not.


> **Note:** Downgrading is not supported.


## Prerequisites

### Required Tools

- [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform)
- [Google Cloud CLI](https://cloud.google.com/sdk/docs/install)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Helm 3.2.0+](https://helm.sh/docs/intro/install/)

## Upgrade process

> **Important:** The following procedure performs a rolling upgrade, where both the old and new Materialize instances are running before the old instances are removed. When performing a rolling upgrade, ensure you have enough resources to support having both the old and new Materialize instances running.


### Step 1: Set up

1. Open a Terminal window.

1. Configure Google Cloud CLI with your GCP credentials. For details, see the [Google Cloud
   documentation](https://cloud.google.com/sdk/docs/initializing).

1. Go to the Terraform directory for your Materialize deployment. For example,
   if you deployed from the `gcp/examples/simple` directory:

   ```bash
   cd materialize-terraform-self-managed/gcp/examples/simple
   ```

1. Configure `kubectl` to connect to your GKE cluster, replacing:

   - `<your-gke-cluster-name>` with your cluster name; i.e., the
     `gke_cluster_name` in the Terraform output. For the sample example, your
     cluster name has the form `<name_prefix>-gke`; e.g., `simple-demo-gke`

   - `<your-region>` with your cluster location; i.e., the
     `gke_cluster_location` in the Terraform output. Your
     region can also be found in your `terraform.tfvars` file.

   - `<your-project-id>` with your GCP project ID.

   ```bash
   # gcloud container clusters get-credentials <your-gke-cluster-name> --region <your-region> --project <your-project-id>
   gcloud container clusters get-credentials $(terraform output -raw gke_cluster_name) \
    --region $(terraform output -raw gke_cluster_location) \
    --project <your-project-id>
   ```

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl get nodes
   ```

   For help with `kubectl` commands, see [kubectl Quick reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

### Step 2: Update the Helm Chart

> **Important:** <strong>Always</strong> upgrade the Materialize Operator <strong>before</strong>
> upgrading the Materialize instances.


<p>To update your Materialize Helm Chart repository:</p>
<ol>
<li>
<p>Update the Helm repo:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm repo update materialize
</span></span></code></pre></div></li>
<li>
<p>View the available chart versions:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm search repo materialize/materialize-operator --versions
</span></span></code></pre></div></li>
</ol>


### Step 3: Upgrade the Materialize Operator

> **Important:** <strong>Always</strong> upgrade the Materialize Operator <strong>before</strong>
> upgrading the Materialize instances.


<ol>
<li>
<p>Use <code>helm list</code> to find the release name. The sample example
deployment using the unified Terraform module deploys the Operator in the
<code>materialize</code> namespace.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm list -n materialize
</span></span></code></pre></div><p>Retrieve the release name (corresponds to the <code>name_prefix</code> variable
specified in your <code>terraform.tfvars</code>) associated with the
<code>materialize-operator</code> <strong>CHART</strong>; for example, <code>simple-demo</code> in the following output:</p>
<pre tabindex="0"><code class="language-none" data-lang="none">NAME    	    NAMESPACE    REVISION   UPDATED                               STATUS     CHART                         APP VERSION
simple-demo  materialize  1         2025-12-08 11:39:50.185976 -0500 EST   deployed  materialize-operator-v26.1.0  v26.1.0
</code></pre></li>
<li>
<p>Export your current values to a file to preserve any custom settings:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm get values -n materialize simple-demo -o yaml &gt; my-values.yaml
</span></span></code></pre></div></li>
<li>
<p>Upgrade your Operator. For example, the following upgrades the Operator
to v26.8.0:</p>
> **Note:** For major version upgrades, you can <strong>only</strong> upgrade <strong>one</strong> major version
>    at a time. For example, upgrades from <strong>v26</strong>.1.0 to <strong>v27</strong>.3.0 is
>    permitted but <strong>v26</strong>.1.0 to <strong>v28</strong>.0.0 is not.

<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm upgrade -n materialize simple-demo materialize/materialize-operator  <span class="se">\
</span></span></span><span class="line"><span class="cl">  -f my-values.yaml <span class="se">\
</span></span></span><span class="line"><span class="cl">  --version v26.8.0
</span></span></code></pre></div></li>
<li>
<p>Verify that the Operator is running:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-bash" data-lang="bash"><span class="line"><span class="cl">kubectl -n materialize get all
</span></span></code></pre></div></li>
<li>
<p>Get the <strong>APP VERSION</strong> of the Operator.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">helm list -n materialize
</span></span></code></pre></div><p>The <strong>APP VERSION</strong> will be the value that you will use for upgrading
Materialize instances.</p>
</li>
</ol>


### Step 4: Upgrading Materialize Instances

> **Important:** <strong>Always</strong> upgrade the Materialize Operator <strong>before</strong>
> upgrading the Materialize instances.


<p><strong>After</strong> you have upgraded your Materialize Operator, upgrade your
Materialize instance(s) to the <strong>APP Version</strong> of the Operator. When
upgrading Materialize Instances versions, changes are not automatically
rolled out by the Operator in order to minimize unexpected downtime and
avoid connection drops at critical periods. Instead, the upgrade process
involves two steps:</p>
<ul>
<li>First, staging the version change to the Materialize custom resource.</li>
<li>Second, rolling out the changes via a <code>requestRollout</code> flag.</li>
</ul>
<ol>
<li>
<p>Find the name of the Materialize instance to upgrade. The sample example
deployment using the unified Terraform module deploys the Materialie
instance in the<code>materialize-environment</code> namespace.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">kubectl get materialize -n  materialize-environment
</span></span></code></pre></div><p>In the example deployment, the name of the instance is <code>main</code>.</p>
<pre tabindex="0"><code class="language-none" data-lang="none">NAME
main
</code></pre></li>
<li>
<p>Stage, but not rollout, the Materialize instance version upgrade.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">kubectl patch materialize main<span class="se">\
</span></span></span><span class="line"><span class="cl">  -n materialize-environment <span class="se">\
</span></span></span><span class="line"><span class="cl">  --type<span class="o">=</span><span class="s1">&#39;merge&#39;</span> <span class="se">\
</span></span></span><span class="line"><span class="cl">  -p <span class="s2">&#34;{\&#34;spec\&#34;: {\&#34;environmentdImageRef\&#34;: \&#34;docker.io/materialize/environmentd:v26.8.0\&#34;}}&#34;</span>
</span></span></code></pre></div></li>
<li>
<p>Rollout the Materialize instance version change.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-shell" data-lang="shell"><span class="line"><span class="cl">kubectl patch materialize main <span class="se">\
</span></span></span><span class="line"><span class="cl">  -n materialize-environment <span class="se">\
</span></span></span><span class="line"><span class="cl">  --type<span class="o">=</span><span class="s1">&#39;merge&#39;</span> <span class="se">\
</span></span></span><span class="line"><span class="cl">  -p <span class="s2">&#34;{\&#34;spec\&#34;: {\&#34;requestRollout\&#34;: \&#34;</span><span class="k">$(</span>uuidgen<span class="k">)</span><span class="s2">\&#34;}}&#34;</span>
</span></span></code></pre></div></li>
<li>
<p>Verify the upgrade by checking the <code>environmentd</code> events:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-bash" data-lang="bash"><span class="line"><span class="cl">kubectl -n materialize-environment describe pod -l <span class="nv">app</span><span class="o">=</span>environmentd
</span></span></code></pre></div></li>
</ol>


## See also

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)
- [Troubleshooting](/self-managed-deployments/troubleshooting/)


---

## Upgrade on kind


To upgrade your Materialize instances, first choose a new operator version and upgrade the Materialize operator. Then, upgrade your Materialize instances to the same version. The following tutorial upgrades your Materialize deployment running locally on a [`kind`](https://kind.sigs.k8s.io/)
cluster.

The tutorial assumes you have installed Materialize on `kind` using the
instructions on [Install locally on
kind](/self-managed-deployments/installation/install-on-local-kind/).

> **Important:** When performing major version upgrades, you can upgrade only one major version
> at a time. For example, upgrades from **v26**.1.0 to **v27**.2.0 is permitted
> but **v26**.1.0 to **v28**.0.0 is not. Skipping major versions or downgrading is
> not supported. To upgrade from v25.2 to v26.0, you must [upgrade first to v25.2.16+](https://materialize.com/docs/self-managed/v25.2/release-notes/#v25216).



## Prerequisites

### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, install. For details, see the
[Helm documentation](https://helm.sh/docs/intro/install/).

### `kubectl`

This tutorial uses `kubectl`. To install, refer to the [`kubectl`
documentationq](https://kubernetes.io/docs/tasks/tools/).

For help with `kubectl` commands, see [kubectl Quick
reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

### License key

Starting in v26.0, Materialize requires a license key. If your existing
deployment does not have a license key configured, contact <a href="https://materialize.com/docs/support/" >Materialize support</a>.

## Upgrade

> **Important:** The following procedure performs a rolling upgrade, where both the old and new
> Materialize instances are running before the the old instance are removed.
> When performing a rolling upgrade, ensure you have enough resources to support
> having both the old and new Materialize instances running.





1. Open a Terminal window.

1. Go to your Materialize working directory.

   ```shell
   cd my-local-mz
   ```

1. Upgrade the Materialize Helm chart.

   ```shell
   helm repo update materialize
   ```

1. Get the sample configuration files for the new version.

   ```shell
   mz_version=v26.8.0

   curl -o upgrade-values.yaml https://raw.githubusercontent.com/MaterializeInc/materialize/refs/tags/$mz_version/misc/helm-charts/operator/values.yaml
   ```

   If you have previously modified the `sample-values.yaml` file for your
   deployment, include the changes into the `upgrade-values.yaml` file.

1. Upgrade the Materialize Operator, specifying the new version and the updated
   configuration file. Include any additional configurations that you specify
   for your deployment.

   ```shell
   helm upgrade my-materialize-operator materialize/materialize-operator \
   --namespace=materialize \
   --version v26.8.0 \
   -f upgrade-values.yaml \
   --set observability.podMetrics.enabled=true
   ```

1. Verify that the operator is running:

   ```bash
   kubectl -n materialize get all
   ```

   Verify the operator upgrade by checking its events:

   ```bash
   kubectl -n materialize describe pod -l app.kubernetes.io/name=materialize-operator
   ```

1. As of v26.0, Self-Managed Materialize requires a license key. If your
deployment has not been configured with a license key:

   a. Contact [Materialize support](https://materialize.com/docs/support/).

   b. Once you have your license key, run the following command to add it to the `materialize-backend` secret:

      ```bash
      kubectl -n materialize-environment patch secret materialize-backend -p '{"stringData":{"license_key":"<your license key goes here>"}}' --type=merge
      ```

1. Create a new `upgrade-materialize.yaml` file, updating the following fields:

   | Field | Description |
   |-------|-------------|
   | `environmentdImageRef` | Update the version to the new version. This should be the same as the operator version: `v26.8.0`. |
   | `requestRollout` or `forceRollout`| Enter a new UUID. Can be generated with `uuidgen`. <br> <ul><li>`requestRollout` triggers a rollout only if changes exist. </li><li>`forceRollout` triggers a rollout even if no changes exist.</li></ul> |


   ```yaml
   apiVersion: materialize.cloud/v1alpha1
   kind: Materialize
   metadata:
     name: 12345678-1234-1234-1234-123456789012
     namespace: materialize-environment
   spec:
     environmentdImageRef: materialize/environmentd:v26.8.0 # Update version
     requestRollout: 22222222-2222-2222-2222-222222222222    # Enter a new UUID
   # forceRollout: 33333333-3333-3333-3333-333333333333    # For forced rollouts
     rolloutStrategy: WaitUntilReady                         # The mechanism to use when rolling out the new version.
     backendSecretName: materialize-backend
   ```

1. Apply the upgrade-materialize.yaml file to your Materialize instance:

   ```shell
   kubectl apply -f upgrade-materialize.yaml
   ```

1. Verify that the components are running after the upgrade:

   ```bash
   kubectl -n materialize-environment get all
   ```

   Verify upgrade by checking the `balancerd` events:

   ```bash
   kubectl -n materialize-environment describe pod -l app=balancerd
   ```

   The **Events** section should list that the new version of the `balancerd`
   have been pulled.

   Verify the upgrade by checking the `environmentd` events:

   ```bash
   kubectl -n materialize-environment describe pod -l app=environmentd
   ```

   The **Events** section should list that the new version of the `environmentd`
   have been pulled.

1. Open the Materialize Console. The Console should display the new version.


## See also

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
- [Troubleshooting](/self-managed-deployments/troubleshooting/)
