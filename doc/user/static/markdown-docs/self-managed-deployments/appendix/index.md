# Appendix



## Table of contents

- [Appendix: Cluster sizes](./appendix-cluster-sizes/)
- [Appendix: Prepare for swap and upgrade to v26.0](./upgrade-to-swap/)



---

## Cluster sizes


## Default Cluster Sizes

For Self-Managed Materialize, the cluster sizes are configured with the
following default resource allocations:

<table>
<thead>
<tr>
<th>Size</th>
<th>Scale</th>
<th>CPU Limit</th>
<th>Disk Limit</th>
<th>Memory Limit</th>
</tr>
</thead>
<tbody>

<tr>
<td><code>25cc</code></td>
<td><code>1</code></td>
<td><code>0.5</code></td>
<td><code>7762MiB</code></td>
<td><code>3881MiB</code></td>
</tr>

<tr>
<td><code>50cc</code></td>
<td><code>1</code></td>
<td><code>1</code></td>
<td><code>15525MiB</code></td>
<td><code>7762MiB</code></td>
</tr>

<tr>
<td><code>100cc</code></td>
<td><code>1</code></td>
<td><code>2</code></td>
<td><code>31050MiB</code></td>
<td><code>15525MiB</code></td>
</tr>

<tr>
<td><code>200cc</code></td>
<td><code>1</code></td>
<td><code>4</code></td>
<td><code>62100MiB</code></td>
<td><code>31050MiB</code></td>
</tr>

<tr>
<td><code>300cc</code></td>
<td><code>1</code></td>
<td><code>6</code></td>
<td><code>93150MiB</code></td>
<td><code>46575MiB</code></td>
</tr>

<tr>
<td><code>400cc</code></td>
<td><code>1</code></td>
<td><code>8</code></td>
<td><code>124201MiB</code></td>
<td><code>62100MiB</code></td>
</tr>

<tr>
<td><code>600cc</code></td>
<td><code>1</code></td>
<td><code>12</code></td>
<td><code>186301MiB</code></td>
<td><code>93150MiB</code></td>
</tr>

<tr>
<td><code>800cc</code></td>
<td><code>1</code></td>
<td><code>16</code></td>
<td><code>248402MiB</code></td>
<td><code>124201MiB</code></td>
</tr>

<tr>
<td><code>1200cc</code></td>
<td><code>1</code></td>
<td><code>24</code></td>
<td><code>372603MiB</code></td>
<td><code>186301MiB</code></td>
</tr>

<tr>
<td><code>1600cc</code></td>
<td><code>1</code></td>
<td><code>31</code></td>
<td><code>481280MiB</code></td>
<td><code>240640MiB</code></td>
</tr>

<tr>
<td><code>3200cc</code></td>
<td><code>1</code></td>
<td><code>62</code></td>
<td><code>962560MiB</code></td>
<td><code>481280MiB</code></td>
</tr>

<tr>
<td><code>6400cc</code></td>
<td><code>2</code></td>
<td><code>62</code></td>
<td><code>962560MiB</code></td>
<td><code>481280MiB</code></td>
</tr>

</tbody>
</table>


## Custom Cluster Sizes

When installing the Materialize Helm chart, you can override the [default
cluster sizes and resource allocations](#default-cluster-sizes). These
cluster sizes are used for both internal clusters, such as the `system_cluster`,
as well as user clusters.

> **Tip:** In general, you should not have to override the defaults. At minimum, we
> recommend that you keep the 25-200cc cluster sizes.
>
>


```yaml
operator:
  clusters:
    sizes:
      <size>:
        workers: <int>
        scale: 1                  # Generally, should be set to 1.
        cpu_exclusive: <bool>
        cpu_limit: <float>         # e.g., 6
        credits_per_hour: "0.0"    # N/A for self-managed.
        disk_limit: <string>       # e.g., "93150MiB"
        memory_limit: <string>     # e.g., "46575MiB"
        selectors: <map>           # k8s label selectors
        # ex: kubernetes.io/arch: amd64
```


| Field | Type | Description | Recommendation |
| --- | --- | --- | --- |
| <strong>workers</strong> | int | The number of timely workers in your cluster replica. | Use 1 worker per CPU core, with a minimum of 1 worker. |
| <strong>scale</strong> | int | The number of pods (i.e., processes) to use in a cluster replica; used to scale out replicas horizontally. Each pod will be provisioned using the settings defined in the size definition. | Generally, this should be set to 1. This should only be greater than 1 when a replica needs to take on limits that are greater than the maximum limits permitted on a single node. |
| <strong>cpu_exclusive</strong> | bool | The flag that determines if the workers should attempt to pin to a particular CPU core. | <p><a name="cpu_exclusive"></a></p> <p>Set to true <strong>if and only if</strong> the <a href="#cpu_limit" ><code>cpu_limit</code></a> is a whole number and the CPU management policy in the k8s cluster is set to static.</p>  |
| <strong>cpu_limit</strong> | float | <a name="cpu_limit"></a> The k8s limit for CPU for a replica pod in cores. | <p>Prefer whole number values to enable CPU affinity. Kubernetes only allows CPU Affinity for pods taking a whole number of cores.</p> <p>If the value is not a whole number, set <a href="#cpu_exclusive" ><code>cpu_exclusive</code></a> to false.</p>  |
| <strong>memory_limit</strong> | float | The k8s limit for memory for a replica pod in bytes. | <dl> <dt>For most workloads, use an approximate <strong>1:8</strong> CPU-to-memory ratio (1 core</dt> <dd>8 GiB). This can vary depending on your workload characteristics.</dd> </dl>  |
| <strong>disk_limit</strong> | float | The size of the NVMe persistent volume to provision for a replica pod in bytes. | When spill-to-disk is enabled, use a <strong>1:2</strong> memory-to-disk ratio. Materialize spills data to disk when memory is insufficient, which can impact performance. |
| <strong>credits_per_hour</strong> | string | This is a cloud attribute that should be set to &ldquo;0.00&rdquo; in self-managed. | Set to &ldquo;0.00&rdquo; for self-managed deployments. |
| <strong>selectors</strong> | map | This is a map of selector keys to values that will be used to schedule pods for this cluster on specific nodes. | It is generally not required to set this. |


> **Note:** If you have modified the default cluster size configurations, you can query the
> [`mz_cluster_replica_sizes`](/sql/system-catalog/mz_catalog/#mz_cluster_replica_sizes)
> system catalog table for the specific resource allocations.
>
>



---

## Legacy Terraform: Releases and configurations


## Table of contents


---

## Prepare for swap and upgrade to v26.0


> **Disambiguation:** This page outlines the general steps for upgrading from v25.2 to v26.0 if you are <red>**not**</red> using Materialize provided Terraforms. If you are using Materialize-provided Terraforms, `v0.6.1` and higher of the Terraforms handle the preparation for you.  If using Materialize-provided Terraforms, upgrade your Terraform version to `v0.6.1` or higher and follow the Upgrade notes: - <a href="https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#v061" >AWS Terraform v0.6.1 Upgrade Notes</a>. - <a href="https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#v061" >GCP Terraform v0.6.1 Upgrade Notes</a>. - <a href="https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#v061" >Azure Terraform v0.6.1 Upgrade Notes</a>. See also [Upgrade Overview](/self-managed-deployments/upgrading/).


<p>Starting in v26.0.0, Self-Managed Materialize enables swap by default. Swap
allows for infrequently accessed data to be moved from memory to disk. Enabling
swap reduces the memory required to operate Materialize and improves cost
efficiency.</p>
<p>To facilitate upgrades, Self-Managed Materialize added new labels to the node
selectors for <code>clusterd</code> pods. To upgrade, you must prepare your nodes with the
new labels. This guide provides general instructions for preparing for swap and
upgrading to v26.0.0 if you are <red><strong>not</strong></red> using the
Materialize-provided Terraforms.</p>
<h2 id="upgrade-to-v260-without-materialize-provided-terraforms">Upgrade to v26.0 without Materialize-provided Terraforms</h2>
> **Tip:** <p>Whe upgrading:</p>
> <ul>
> <li>
> <p><strong>Always</strong> check the <a href="/self-managed-deployments/upgrading/#version-specific-upgrade-notes" >version specific upgrade
> notes</a>.</p>
> </li>
> <li>
> <p><strong>Always</strong> upgrade the operator <strong>first</strong> and ensure version compatibility
> between the operator and the Materialize instance you are upgrading to.</p>
> </li>
> <li>
> <p><strong>Always</strong> upgrade your Materialize instances <strong>after</strong> upgrading the operator
> to ensure compatibility.</p>
> </li>
> </ul>
>
>
> See also [General notes for upgrades](/self-managed-deployments/upgrading/)
>

<ol>
<li>
<p>Label existing scratchfs/lgalloc node groups.</p>
<p>If using lgalloc on scratchfs volumes, add the additional
<code>&quot;materialize.cloud/scratch-fs&quot;: &quot;true&quot;</code> label to your existing node groups
and nodes running Materialize workloads.</p>
<p>Adding this label to the node group (or nodepool) configuration will apply the label to newly spawned nodes, but depending on your cloud provider may not apply the label to existing nodes.</p>
<p>If not automatically applied, you may need to use <code>kubectl label</code> to apply the change to existing nodes.</p>
</li>
<li>
<p>Modify existing scratchfs/lgalloc disk setup daemonset selector labels</p>
<p>If using our <a href="https://github.com/MaterializeInc/ephemeral-storage-setup-image/" >ephemeral-storage-setup image</a> as a daemonset to configure scratchfs LVM volumes for lgalloc, you must add the additional <code>&quot;materialize.cloud/scratch-fs&quot;: &quot;true&quot;</code> label to multiple places:</p>
<ul>
<li><code>spec.selector.matchLabels</code></li>
<li><code>spec.template.metadata.labels</code></li>
<li>(if using <code>nodeAffinity</code>) <code>spec.template.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms</code></li>
<li>(if using <code>nodeSelector</code>) <code>spec.template.spec.nodeSelector</code></li>
</ul>
<p>You <strong>must</strong> use at least one of <code>nodeAffinity</code> or <code>nodeSelector</code>.</p>
<p>It is recommended to rename this daemonset to make it clear that it is only for the legacy scratchfs/lgalloc nodes (for example, change the name <code>disk-setup</code> to <code>disk-setup-scratchfs</code>).</p>
</li>
<li>
<p>Create a new node group for swap</p>
<ol>
<li>
<p>Create a new node group (or ec2nodeclass and nodepool if using Karpenter in AWS) using an instance type with local NVMe disks. If in GCP, the disks must be in <code>raw</code> mode.</p>
</li>
<li>
<p>Label the node group with <code>&quot;materialize.cloud/swap&quot;: &quot;true&quot;</code>.</p>
</li>
<li>
<p>If using AWS Bottlerocket AMIs (highly recommended if running in AWS), set the following in the userdata to configure the disks for swap, and enable swap in the kubelet:</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-toml" data-lang="toml"><span class="line"><span class="cl"><span class="p">[</span><span class="nx">settings</span><span class="p">.</span><span class="nx">oci-defaults</span><span class="p">.</span><span class="nx">resource-limits</span><span class="p">.</span><span class="nx">max-open-files</span><span class="p">]</span>
</span></span><span class="line"><span class="cl"><span class="nx">soft-limit</span> <span class="p">=</span> <span class="mi">1048576</span>
</span></span><span class="line"><span class="cl"><span class="nx">hard-limit</span> <span class="p">=</span> <span class="mi">1048576</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="p">[</span><span class="nx">settings</span><span class="p">.</span><span class="nx">bootstrap-containers</span><span class="p">.</span><span class="nx">diskstrap</span><span class="p">]</span>
</span></span><span class="line"><span class="cl"><span class="nx">source</span> <span class="p">=</span> <span class="s2">&#34;docker.io/materialize/ephemeral-storage-setup-image:v0.4.0&#34;</span>
</span></span><span class="line"><span class="cl"><span class="nx">mode</span> <span class="p">=</span> <span class="s2">&#34;once&#34;</span>
</span></span><span class="line"><span class="cl"><span class="nx">essential</span> <span class="p">=</span> <span class="s2">&#34;true&#34;</span>
</span></span><span class="line"><span class="cl"><span class="c"># [&#34;swap&#34;, &#34;--cloud-provider&#34;, &#34;aws&#34;, &#34;--bottlerocket-enable-swap&#34;]</span>
</span></span><span class="line"><span class="cl"><span class="nx">user-data</span> <span class="p">=</span> <span class="s2">&#34;WyJzd2FwIiwgIi0tY2xvdWQtcHJvdmlkZXIiLCAiYXdzIiwgIi0tYm90dGxlcm9ja2V0LWVuYWJsZS1zd2FwIl0=&#34;</span>
</span></span><span class="line"><span class="cl">
</span></span><span class="line"><span class="cl"><span class="p">[</span><span class="nx">kernel</span><span class="p">.</span><span class="nx">sysctl</span><span class="p">]</span>
</span></span><span class="line"><span class="cl"><span class="s2">&#34;vm.swappiness&#34;</span> <span class="p">=</span> <span class="s2">&#34;100&#34;</span>
</span></span><span class="line"><span class="cl"><span class="s2">&#34;vm.min_free_kbytes&#34;</span> <span class="p">=</span> <span class="s2">&#34;1048576&#34;</span>
</span></span><span class="line"><span class="cl"><span class="s2">&#34;vm.watermark_scale_factor&#34;</span> <span class="p">=</span> <span class="s2">&#34;100&#34;</span>
</span></span></code></pre></div></li>
<li>
<p>If not using AWS or not using Bottlerocket AMIs, and your node group supports it (Azure does not as of 2025-11-05), add a startup taint. This taint will be removed after the disk is configured for swap.</p>
<div class="highlight"><pre tabindex="0" class="chroma"><code class="language-yaml" data-lang="yaml"><span class="line"><span class="cl"><span class="nt">taints</span><span class="p">:</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w">  </span>- <span class="nt">key</span><span class="p">:</span><span class="w"> </span><span class="l">startup-taint.cluster-autoscaler.kubernetes.io/disk-unconfigured</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w">    </span><span class="nt">value</span><span class="p">:</span><span class="w"> </span><span class="s2">&#34;true&#34;</span><span class="w">
</span></span></span><span class="line"><span class="cl"><span class="w">    </span><span class="nt">effect</span><span class="p">:</span><span class="w"> </span><span class="l">NoSchedule</span><span class="w">
</span></span></span></code></pre></div></li>
</ol>
</li>
<li>
<p>Create a new disk-setup-swap daemonset</p>
<p>If using Bottlerocket AMIs in AWS, you may skip this step, as you should have configured swap using userdata previously.</p>
<p>Create a new daemonset using our <a href="https://github.com/MaterializeInc/ephemeral-storage-setup-image/" >ephemeral-storage-setup image</a> to configure the disks for swap and to enable swap in the kubelet.</p>
<p>The arguments to the init container in this daemonset need to be configured for swap. See the examples in the linked git repository for more details.</p>
<p>This daemonset should run only on the new swap nodes, so we need to ensure it has the <code>&quot;materialize.cloud/swap&quot;: &quot;true&quot;</code> label in several places:</p>
<ul>
<li><code>spec.selector.matchLabels</code></li>
<li><code>spec.template.metadata.labels</code></li>
<li>(if using <code>nodeAffinity</code>) <code>spec.template.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms</code></li>
<li>(if using <code>nodeSelector</code>) <code>spec.template.spec.nodeSelector</code></li>
</ul>
<p>You <strong>must</strong> use at least one of <code>nodeAffinity</code> or <code>nodeSelector</code>.</p>
<p>It is recommended to name this daemonset to clearly indicate that it is for configuring swap (ie: <code>disk-setup-swap</code>), as opposed to other disk configurations.</p>
</li>
<li>
<p>(Optional) Configure environmentd to also use swap</p>
<p>Swap is enabled by default for clusterd, but not for environmentd. If you&rsquo;d like to enable swap for environmentd, add <code>&quot;materialize.cloud/swap&quot;: &quot;true&quot;</code> to the <code>environmentd.node_selector</code> helm value.</p>
</li>
<li>
<p>Upgrade the Materialize operator helm chart to v26</p>
<p>The cluster size definitions for existing Materialize instances will not be changed at this point, but any newly created Materialize instances, or upgraded Materialize instances will pick up the new sizes.</p>
<p>Do not create any new Materialize instances at versions less than v26, or perform any rollouts to existing Materialize instances to versions less than v26.</p>
</li>
<li>
<p>Upgrade existing Materialize instances to v26</p>
<p>The new v26 pods should go to the new swap nodes.</p>
<p>You can verify that swap is enabled and working by <code>exec</code>ing into a clusterd pod and running <code>cat /sys/fs/cgroup/memory.swap.max</code>. If you get a number greater than 0, swap is enabled and the pod is allowed to use it.</p>
</li>
<li>
<p>(Optional) Delete old scratchfs/lgalloc node groups and disk-setup-scratchfs daemonset</p>
<p>If you no longer have anything running on the old scratchfs/lgalloc nodes, you may delete their node group and the disk-setup-scratchfs daemonset.</p>
</li>
</ol>
<h2 id="how-to-disable-swap">How to disable swap</h2>
<p>If you wish to opt out of swap and retain the old behavior, you may set <code>operator.clusters.swap_enabled: false</code> in your Helm values.</p>
