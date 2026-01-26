# Prepare for swap and upgrade to v26.0

Upgrade procedure for v26.0 if not using Materialize Terraform.



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
