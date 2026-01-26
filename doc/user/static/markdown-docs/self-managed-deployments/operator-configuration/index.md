# Materialize Operator Configuration

Configuration reference for the Materialize Operator Helm chart



## Configure the Materialize operator

To configure the Materialize operator, you can:

- Use a configuration YAML file (e.g., `values.yaml`) that specifies the
  configuration values and then install the chart with the `-f` flag:

  ```shell
  # Assumes you have added the Materialize operator Helm chart repository
  helm install my-materialize-operator materialize/materialize-operator \
     -f /path/to/your/config/values.yaml
  ```

- Specify each parameter using the `--set key=value[,key=value]` argument to
  `helm install`. For example:

  ```shell
  # Assumes you have added the Materialize operator Helm chart repository
  helm install my-materialize-operator materialize/materialize-operator  \
    --set observability.podMetrics.enabled=true
  ```


<table>
<thead>
<tr>
<th>Parameter</th>
<th>Default</th>

</tr>
</thead>
<tbody>

<tr>
<td><a href='#balancerdaffinity'><code>balancerd.affinity</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#balancerddefaultresourceslimits'><code>balancerd.defaultResources.limits</code></a></td>
<td>
<code>{&quot;memory&quot;:&quot;256Mi&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#balancerddefaultresourcesrequests'><code>balancerd.defaultResources.requests</code></a></td>
<td>
<code>{&quot;cpu&quot;:&quot;500m&quot;,&quot;memory&quot;:&quot;256Mi&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#balancerdenabled'><code>balancerd.enabled</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#balancerdnodeselector'><code>balancerd.nodeSelector</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#balancerdtolerations'><code>balancerd.tolerations</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#clusterdaffinity'><code>clusterd.affinity</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#clusterdnodeselector'><code>clusterd.nodeSelector</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#clusterdscratchfsnodeselector'><code>clusterd.scratchfsNodeSelector</code></a></td>
<td>
<code>{&quot;materialize.cloud/scratch-fs&quot;:&quot;true&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#clusterdswapnodeselector'><code>clusterd.swapNodeSelector</code></a></td>
<td>
<code>{&quot;materialize.cloud/swap&quot;:&quot;true&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#clusterdtolerations'><code>clusterd.tolerations</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#consoleaffinity'><code>console.affinity</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#consoledefaultresourceslimits'><code>console.defaultResources.limits</code></a></td>
<td>
<code>{&quot;memory&quot;:&quot;256Mi&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#consoledefaultresourcesrequests'><code>console.defaultResources.requests</code></a></td>
<td>
<code>{&quot;cpu&quot;:&quot;500m&quot;,&quot;memory&quot;:&quot;256Mi&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#consoleenabled'><code>console.enabled</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#consoleimagetagmapoverride'><code>console.imageTagMapOverride</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#consolenodeselector'><code>console.nodeSelector</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#consoletolerations'><code>console.tolerations</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#environmentdaffinity'><code>environmentd.affinity</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#environmentddefaultresourceslimits'><code>environmentd.defaultResources.limits</code></a></td>
<td>
<code>{&quot;memory&quot;:&quot;4Gi&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#environmentddefaultresourcesrequests'><code>environmentd.defaultResources.requests</code></a></td>
<td>
<code>{&quot;cpu&quot;:&quot;1&quot;,&quot;memory&quot;:&quot;4095Mi&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#environmentdnodeselector'><code>environmentd.nodeSelector</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#environmentdtolerations'><code>environmentd.tolerations</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#networkpoliciesegresscidrs'><code>networkPolicies.egress.cidrs</code></a></td>
<td>
<code>[&quot;0.0.0.0/0&quot;]</code>
</td>
</tr>

<tr>
<td><a href='#networkpoliciesegressenabled'><code>networkPolicies.egress.enabled</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#networkpoliciesenabled'><code>networkPolicies.enabled</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#networkpoliciesingresscidrs'><code>networkPolicies.ingress.cidrs</code></a></td>
<td>
<code>[&quot;0.0.0.0/0&quot;]</code>
</td>
</tr>

<tr>
<td><a href='#networkpoliciesingressenabled'><code>networkPolicies.ingress.enabled</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#networkpoliciesinternalenabled'><code>networkPolicies.internal.enabled</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#observabilityenabled'><code>observability.enabled</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#observabilitypodmetricsenabled'><code>observability.podMetrics.enabled</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#observabilityprometheusscrapeannotationsenabled'><code>observability.prometheus.scrapeAnnotations.enabled</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#operatoradditionalmaterializecrdcolumns'><code>operator.additionalMaterializeCRDColumns</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#operatoraffinity'><code>operator.affinity</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#operatorargsenableinternalstatementlogging'><code>operator.args.enableInternalStatementLogging</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#operatorargsenablelicensekeychecks'><code>operator.args.enableLicenseKeyChecks</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#operatorargsstartuplogfilter'><code>operator.args.startupLogFilter</code></a></td>
<td>
<code>&quot;INFO,mz_orchestratord=TRACE&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorcloudproviderprovidersawsaccountid'><code>operator.cloudProvider.providers.aws.accountID</code></a></td>
<td>
<code>&quot;&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorcloudproviderprovidersawsenabled'><code>operator.cloudProvider.providers.aws.enabled</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#operatorcloudproviderprovidersawsiamrolesconnection'><code>operator.cloudProvider.providers.aws.iam.roles.connection</code></a></td>
<td>
<code>&quot;&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorcloudproviderprovidersawsiamrolesenvironment'><code>operator.cloudProvider.providers.aws.iam.roles.environment</code></a></td>
<td>
<code>&quot;&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorcloudproviderprovidersgcp'><code>operator.cloudProvider.providers.gcp</code></a></td>
<td>
<code>{&quot;enabled&quot;:false}</code>
</td>
</tr>

<tr>
<td><a href='#operatorcloudproviderregion'><code>operator.cloudProvider.region</code></a></td>
<td>
<code>&quot;kind&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorcloudprovidertype'><code>operator.cloudProvider.type</code></a></td>
<td>
<code>&quot;local&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultreplicationfactoranalytics'><code>operator.clusters.defaultReplicationFactor.analytics</code></a></td>
<td>
<code>0</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultreplicationfactorprobe'><code>operator.clusters.defaultReplicationFactor.probe</code></a></td>
<td>
<code>0</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultreplicationfactorsupport'><code>operator.clusters.defaultReplicationFactor.support</code></a></td>
<td>
<code>0</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultreplicationfactorsystem'><code>operator.clusters.defaultReplicationFactor.system</code></a></td>
<td>
<code>0</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultsizesanalytics'><code>operator.clusters.defaultSizes.analytics</code></a></td>
<td>
<code>&quot;25cc&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultsizescatalogserver'><code>operator.clusters.defaultSizes.catalogServer</code></a></td>
<td>
<code>&quot;25cc&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultsizesdefault'><code>operator.clusters.defaultSizes.default</code></a></td>
<td>
<code>&quot;25cc&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultsizesprobe'><code>operator.clusters.defaultSizes.probe</code></a></td>
<td>
<code>&quot;mz_probe&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultsizessupport'><code>operator.clusters.defaultSizes.support</code></a></td>
<td>
<code>&quot;25cc&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersdefaultsizessystem'><code>operator.clusters.defaultSizes.system</code></a></td>
<td>
<code>&quot;25cc&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorclustersswap_enabled'><code>operator.clusters.swap_enabled</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#operatorimagepullpolicy'><code>operator.image.pullPolicy</code></a></td>
<td>
<code>&quot;IfNotPresent&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorimagerepository'><code>operator.image.repository</code></a></td>
<td>
<code>&quot;materialize/orchestratord&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatorimagetag'><code>operator.image.tag</code></a></td>
<td>
<code>&quot;v26.8.0&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatornodeselector'><code>operator.nodeSelector</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#operatorresourceslimits'><code>operator.resources.limits</code></a></td>
<td>
<code>{&quot;memory&quot;:&quot;512Mi&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#operatorresourcesrequests'><code>operator.resources.requests</code></a></td>
<td>
<code>{&quot;cpu&quot;:&quot;100m&quot;,&quot;memory&quot;:&quot;512Mi&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#operatorsecretscontroller'><code>operator.secretsController</code></a></td>
<td>
<code>&quot;kubernetes&quot;</code>
</td>
</tr>

<tr>
<td><a href='#operatortolerations'><code>operator.tolerations</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

<tr>
<td><a href='#rbaccreate'><code>rbac.create</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#schedulername'><code>schedulerName</code></a></td>
<td>
<code>nil</code>
</td>
</tr>

<tr>
<td><a href='#serviceaccountcreate'><code>serviceAccount.create</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#serviceaccountname'><code>serviceAccount.name</code></a></td>
<td>
<code>&quot;orchestratord&quot;</code>
</td>
</tr>

<tr>
<td><a href='#storagestorageclassallowvolumeexpansion'><code>storage.storageClass.allowVolumeExpansion</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#storagestorageclasscreate'><code>storage.storageClass.create</code></a></td>
<td>
<code>false</code>
</td>
</tr>

<tr>
<td><a href='#storagestorageclassname'><code>storage.storageClass.name</code></a></td>
<td>
<code>&quot;&quot;</code>
</td>
</tr>

<tr>
<td><a href='#storagestorageclassparameters'><code>storage.storageClass.parameters</code></a></td>
<td>
<code>{&quot;fsType&quot;:&quot;ext4&quot;,&quot;storage&quot;:&quot;lvm&quot;,&quot;volgroup&quot;:&quot;instance-store-vg&quot;}</code>
</td>
</tr>

<tr>
<td><a href='#storagestorageclassprovisioner'><code>storage.storageClass.provisioner</code></a></td>
<td>
<code>&quot;&quot;</code>
</td>
</tr>

<tr>
<td><a href='#storagestorageclassreclaimpolicy'><code>storage.storageClass.reclaimPolicy</code></a></td>
<td>
<code>&quot;Delete&quot;</code>
</td>
</tr>

<tr>
<td><a href='#storagestorageclassvolumebindingmode'><code>storage.storageClass.volumeBindingMode</code></a></td>
<td>
<code>&quot;WaitForFirstConsumer&quot;</code>
</td>
</tr>

<tr>
<td><a href='#telemetryenabled'><code>telemetry.enabled</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#telemetrysegmentapikey'><code>telemetry.segmentApiKey</code></a></td>
<td>
<code>&quot;hMWi3sZ17KFMjn2sPWo9UJGpOQqiba4A&quot;</code>
</td>
</tr>

<tr>
<td><a href='#telemetrysegmentclientside'><code>telemetry.segmentClientSide</code></a></td>
<td>
<code>true</code>
</td>
</tr>

<tr>
<td><a href='#tlsdefaultcertificatespecs'><code>tls.defaultCertificateSpecs</code></a></td>
<td>
<code>{}</code>
</td>
</tr>

</tbody>
</table>


## Parameters







### `balancerd` parameters



#### balancerd.affinity

**Default**: <code>{}</code>

Affinity to use for balancerd pods spawned by the operator









#### balancerd.defaultResources.limits

**Default**: <code>{&quot;memory&quot;:&quot;256Mi&quot;}</code>

Default resource limits for balancerd&rsquo;s CPU and memory if not set in the Materialize CR









#### balancerd.defaultResources.requests

**Default**: <code>{&quot;cpu&quot;:&quot;500m&quot;,&quot;memory&quot;:&quot;256Mi&quot;}</code>

Default resources requested for balancerd&rsquo;s CPU and memory if not set in the Materialize CR









#### balancerd.enabled

**Default**: <code>true</code>

Flag to indicate whether to create balancerd pods for the environments









#### balancerd.nodeSelector

**Default**: <code>{}</code>

Node selector to use for balancerd pods spawned by the operator









#### balancerd.tolerations

**Default**: <code>{}</code>

Tolerations to use for balancerd pods spawned by the operator







### `clusterd` parameters



#### clusterd.affinity

**Default**: <code>{}</code>

Affinity to use for clusterd pods spawned by the operator









#### clusterd.nodeSelector

**Default**: <code>{}</code>

Node selector to use for all clusterd pods spawned by the operator









#### clusterd.scratchfsNodeSelector

**Default**: <code>{&quot;materialize.cloud/scratch-fs&quot;:&quot;true&quot;}</code>

Additional node selector to use for clusterd pods when using an LVM scratch disk. This will be merged with the values in <code>nodeSelector</code>.









#### clusterd.swapNodeSelector

**Default**: <code>{&quot;materialize.cloud/swap&quot;:&quot;true&quot;}</code>

Additional node selector to use for clusterd pods when using swap. This will be merged with the values in <code>nodeSelector</code>.









#### clusterd.tolerations

**Default**: <code>{}</code>

Tolerations to use for clusterd pods spawned by the operator







### `console` parameters



#### console.affinity

**Default**: <code>{}</code>

Affinity to use for console pods spawned by the operator









#### console.defaultResources.limits

**Default**: <code>{&quot;memory&quot;:&quot;256Mi&quot;}</code>

Default resource limits for the console&rsquo;s CPU and memory if not set in the Materialize CR









#### console.defaultResources.requests

**Default**: <code>{&quot;cpu&quot;:&quot;500m&quot;,&quot;memory&quot;:&quot;256Mi&quot;}</code>

Default resources requested for the console&rsquo;s CPU and memory if not set in the Materialize CR









#### console.enabled

**Default**: <code>true</code>

Flag to indicate whether to create console pods for the environments









#### console.imageTagMapOverride

**Default**: <code>{}</code>

Override the mapping of environmentd versions to console versions









#### console.nodeSelector

**Default**: <code>{}</code>

Node selector to use for console pods spawned by the operator









#### console.tolerations

**Default**: <code>{}</code>

Tolerations to use for console pods spawned by the operator







### `environmentd` parameters



#### environmentd.affinity

**Default**: <code>{}</code>

Affinity to use for environmentd pods spawned by the operator









#### environmentd.defaultResources.limits

**Default**: <code>{&quot;memory&quot;:&quot;4Gi&quot;}</code>

Default resource limits for environmentd&rsquo;s CPU and memory if not set in the Materialize CR









#### environmentd.defaultResources.requests

**Default**: <code>{&quot;cpu&quot;:&quot;1&quot;,&quot;memory&quot;:&quot;4095Mi&quot;}</code>

Default resources requested for environmentd&rsquo;s CPU and memory if not set in the Materialize CR









#### environmentd.nodeSelector

**Default**: <code>{}</code>

Node selector to use for environmentd pods spawned by the operator









#### environmentd.tolerations

**Default**: <code>{}</code>

Tolerations to use for environmentd pods spawned by the operator







### `networkPolicies` parameters



#### networkPolicies.egress.cidrs

**Default**: <code>[&quot;0.0.0.0/0&quot;]</code>

CIDR blocks to allow egress to









#### networkPolicies.egress.enabled

**Default**: <code>false</code>

Whether to enable egress network policies to sources and sinks









#### networkPolicies.enabled

**Default**: <code>false</code>

Whether to enable network policies for securing communication between pods









#### networkPolicies.ingress.cidrs

**Default**: <code>[&quot;0.0.0.0/0&quot;]</code>

CIDR blocks to allow ingress from









#### networkPolicies.ingress.enabled

**Default**: <code>false</code>

Whether to enable ingress network policies to the SQL and HTTP interfaces on environmentd and balancerd









#### networkPolicies.internal.enabled

**Default**: <code>false</code>

Whether to enable network policies for internal communication between Materialize pods







### `observability` parameters



#### observability.enabled

**Default**: <code>true</code>

Whether to enable observability features









#### observability.podMetrics.enabled

**Default**: <code>false</code>

Whether to enable the pod metrics scraper which populates the Environment Overview Monitoring tab in the web console (requires metrics-server to be installed)









#### observability.prometheus.scrapeAnnotations.enabled

**Default**: <code>true</code>

Whether to annotate pods with common keys used for prometheus scraping.







### `operator` parameters



#### operator.additionalMaterializeCRDColumns

**Default**: <code>{}</code>

Additional columns to display when printing the Materialize CRD in table format.









#### operator.affinity

**Default**: <code>{}</code>

Affinity to use for the operator pod









#### operator.args.enableInternalStatementLogging

**Default**: <code>true</code>











#### operator.args.enableLicenseKeyChecks

**Default**: <code>false</code>











#### operator.args.startupLogFilter

**Default**: <code>&quot;INFO,mz_orchestratord=TRACE&quot;</code>

Log filtering settings for startup logs









#### operator.cloudProvider.providers.aws.accountID

**Default**: <code>&quot;&quot;</code>

When using AWS, accountID is required









#### operator.cloudProvider.providers.aws.enabled

**Default**: <code>false</code>











#### operator.cloudProvider.providers.aws.iam.roles.connection

**Default**: <code>&quot;&quot;</code>

ARN for CREATE CONNECTION feature









#### operator.cloudProvider.providers.aws.iam.roles.environment

**Default**: <code>&quot;&quot;</code>

ARN of the IAM role for environmentd









#### operator.cloudProvider.providers.gcp

**Default**: <code>{&quot;enabled&quot;:false}</code>

GCP Configuration (placeholder for future use)









#### operator.cloudProvider.region

**Default**: <code>&quot;kind&quot;</code>

Common cloud provider settings









#### operator.cloudProvider.type

**Default**: <code>&quot;local&quot;</code>

Specifies cloud provider. Valid values are &lsquo;aws&rsquo;, &lsquo;gcp&rsquo;, &lsquo;azure&rsquo; , &lsquo;generic&rsquo;, or &rsquo;local&rsquo;









#### operator.clusters.defaultReplicationFactor.analytics

**Default**: <code>0</code>











#### operator.clusters.defaultReplicationFactor.probe

**Default**: <code>0</code>











#### operator.clusters.defaultReplicationFactor.support

**Default**: <code>0</code>











#### operator.clusters.defaultReplicationFactor.system

**Default**: <code>0</code>











#### operator.clusters.defaultSizes.analytics

**Default**: <code>&quot;25cc&quot;</code>











#### operator.clusters.defaultSizes.catalogServer

**Default**: <code>&quot;25cc&quot;</code>











#### operator.clusters.defaultSizes.default

**Default**: <code>&quot;25cc&quot;</code>











#### operator.clusters.defaultSizes.probe

**Default**: <code>&quot;mz_probe&quot;</code>











#### operator.clusters.defaultSizes.support

**Default**: <code>&quot;25cc&quot;</code>











#### operator.clusters.defaultSizes.system

**Default**: <code>&quot;25cc&quot;</code>











#### operator.clusters.swap_enabled

**Default**: <code>true</code>

Configure sizes such that the pod QoS class is not Guaranteed, as is required for swap to be enabled. Disk doesn&rsquo;t make much sense with swap, as swap performs better than lgalloc, so it also gets disabled.









#### operator.image.pullPolicy

**Default**: <code>&quot;IfNotPresent&quot;</code>

Policy for pulling the image: &ldquo;IfNotPresent&rdquo; avoids unnecessary re-pulling of images









#### operator.image.repository

**Default**: <code>&quot;materialize/orchestratord&quot;</code>

The Docker repository for the operator image









#### operator.image.tag

**Default**: <code>&quot;v26.8.0&quot;</code>

The tag/version of the operator image to be used









#### operator.nodeSelector

**Default**: <code>{}</code>

Node selector to use for the operator pod









#### operator.resources.limits

**Default**: <code>{&quot;memory&quot;:&quot;512Mi&quot;}</code>

Resource limits for the operator&rsquo;s CPU and memory









#### operator.resources.requests

**Default**: <code>{&quot;cpu&quot;:&quot;100m&quot;,&quot;memory&quot;:&quot;512Mi&quot;}</code>

Resources requested by the operator for CPU and memory









#### operator.secretsController

**Default**: <code>&quot;kubernetes&quot;</code>

Which secrets controller to use for storing secrets. Valid values are &lsquo;kubernetes&rsquo; and &lsquo;aws-secrets-manager&rsquo;. Setting &lsquo;aws-secrets-manager&rsquo; requires a configured AWS cloud provider and IAM role for the environment with Secrets Manager permissions.









#### operator.tolerations

**Default**: <code>{}</code>

Tolerations to use for the operator pod







### `rbac` parameters



#### rbac.create

**Default**: <code>true</code>

Whether to create necessary RBAC roles and bindings







### `schedulerName` parameters



#### schedulerName

**Default**: <code>nil</code>

Optionally use a non-default kubernetes scheduler.







### `serviceAccount` parameters



#### serviceAccount.create

**Default**: <code>true</code>

Whether to create a new service account for the operator









#### serviceAccount.name

**Default**: <code>&quot;orchestratord&quot;</code>

The name of the service account to be created







### `storage` parameters



#### storage.storageClass.allowVolumeExpansion

**Default**: <code>false</code>











#### storage.storageClass.create

**Default**: <code>false</code>

Set to false to use an existing StorageClass instead. Refer to the <a href="https://kubernetes.io/docs/concepts/storage/storage-classes/" >Kubernetes StorageClass documentation</a>









#### storage.storageClass.name

**Default**: <code>&quot;&quot;</code>

Name of the StorageClass to create/use: eg &ldquo;openebs-lvm-instance-store-ext4&rdquo;









#### storage.storageClass.parameters

**Default**: <code>{&quot;fsType&quot;:&quot;ext4&quot;,&quot;storage&quot;:&quot;lvm&quot;,&quot;volgroup&quot;:&quot;instance-store-vg&quot;}</code>

Parameters for the CSI driver









#### storage.storageClass.provisioner

**Default**: <code>&quot;&quot;</code>

CSI driver to use, eg &ldquo;local.csi.openebs.io&rdquo;









#### storage.storageClass.reclaimPolicy

**Default**: <code>&quot;Delete&quot;</code>











#### storage.storageClass.volumeBindingMode

**Default**: <code>&quot;WaitForFirstConsumer&quot;</code>









### `telemetry` parameters



#### telemetry.enabled

**Default**: <code>true</code>











#### telemetry.segmentApiKey

**Default**: <code>&quot;hMWi3sZ17KFMjn2sPWo9UJGpOQqiba4A&quot;</code>











#### telemetry.segmentClientSide

**Default**: <code>true</code>









### `tls` parameters



#### tls.defaultCertificateSpecs

**Default**: <code>{}</code>






## See also

- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)
