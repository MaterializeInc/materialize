# Upgrade on GCP
Upgrade Materialize on GCP using the new Terraform module.
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
