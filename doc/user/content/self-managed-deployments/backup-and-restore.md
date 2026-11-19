---
title: "Backup and restore"
description: "Back up a Self-Managed Materialize instance and restore it to an earlier point in time."
menu:
  main:
    parent: "sm-deployments"
    name: "Backup and restore"
    weight: 75
---

Materialize keeps its durable state in two places, both set in the instance's
backend secret:

- The **metadata database** (PostgreSQL, `metadata_backend_url`). Its
  `consensus` schema records which blobs make up each shard's current state,
  and its `tsoracle` schema holds the timestamp oracle.
- The **blob store** (for example, S3, `persist_backend_url`), which holds the
  data as immutable blobs.

Materialize finds its catalog shard from the instance's environment ID
(`spec.environmentId`), and the catalog records every other shard. An instance
finds its data only when its environment ID and both URLs match the original.
With a different environment ID, it starts a new, empty environment in the
same database and bucket.

A backup is a copy of the metadata database. Restoring it rolls consensus back
to a state that references blobs Materialize has since compacted away. Object
versioning keeps those deleted blobs as noncurrent versions, and
`persistcli admin restore-blob` brings them back. Everything written after the
backup is lost. See [Data loss and upstream systems](#data-loss-and-upstream-systems).

## Prerequisites

### Blob store versioning

Turn on object versioning for the persist bucket before Materialize first
starts. Versioning only protects blobs deleted after you turn it on. Then set
these lifecycle rules:

| Rule | Recommendation | Why |
|------|----------------|-----|
| Expire noncurrent versions | Backup retention plus at least 7 days, for example **14 days** for 7 days of backups. | A restore needs every blob its backup references. The margin covers the time to notice a problem and carry out the restore. |
| Remove expired delete markers | Enable (`ExpiredObjectDeleteMarker` on S3). | Once the noncurrent versions expire, the leftover delete markers only slow down listings. |
| Abort incomplete multipart uploads | After 1 day. | Interrupted uploads otherwise keep their parts forever. |
| Expire current versions | **Never.** | Materialize deletes its own blobs. Expiring a current version deletes live data. |
| Transition to colder storage classes | Don't. | Materialize reads every live blob on restart, so colder classes only add retrieval fees and latency. |

Noncurrent versions cost storage in proportion to how much data Materialize
compacts away per day. Watch their size, and size the retention window with
that cost in mind.

The AWS `storage` module of the [Materialize Terraform modules](https://github.com/MaterializeInc/materialize-terraform-self-managed)
turns on versioning and the multipart-upload rule by default. Its default
noncurrent expiry of 7 days equals the `database` module's default backup
retention, which leaves no margin, so raise it:

```hcl
module "storage" {
  # ...
  enable_bucket_versioning           = true
  noncurrent_version_expiration_days = 14
}
```

Versioning doesn't protect against losing the bucket or the account. For that,
replicate the bucket to another region or account.

### Metadata database backups

Any PostgreSQL backup method that produces a consistent copy works, and you
don't need to stop Materialize. We recommend:

- **Continuous backups with point-in-time recovery**, kept 7 to 14 days. On
  RDS, this is `backup_retention_period` in the Terraform `database` module
  (default 7 days).
- **An on-demand backup before every upgrade or other planned change**, such as
  an RDS snapshot or a `pg_dump`.

Without point-in-time recovery, take an hourly `pg_dump` and keep the dumps for
7 to 14 days. The database holds only metadata, so dumps are small:

```shell
pg_dump --format=custom --file=materialize-$(date -u +%Y%m%dT%H%M%SZ).dump \
  "$METADATA_BACKEND_URL"
```

Use `pg_dump` and `pg_restore` from the server's major version. A newer
`pg_restore` can emit settings that an older server rejects, for example
`SET transaction_timeout` against PostgreSQL 16 or older.

Record which Materialize version was running when each backup was taken, for
example in the dump's file name or the snapshot's tags. For point-in-time
recovery, keep a log of when each upgrade finished. A restore must run at the
backup's version (see [step 3](#step-3-restore-blobs)), not at the version
running when you restore. Don't pick a restore point during an upgrade.

### Deployment configuration

A redeploy comes back as the same environment, on the same data, only if it
points at that data exactly as before. Keep the following outside the
Kubernetes cluster, for example in a secret manager:

- **The backend secret**: `metadata_backend_url`, `persist_backend_url`, and
  `license_key`. The path in `persist_backend_url` is part of the location: the
  same bucket under another path holds no data.
- **The environment ID.** If your license key carries one, the operator uses it
  and rejects any other, so keeping the key keeps the ID. Otherwise, the
  operator generates a random ID when you first create the instance. Read it
  back with `kubectl get materialize <name> -n <namespace> -o
  jsonpath='{.spec.environmentId}'`, and set it in your manifest, or in the
  Terraform module's `environment_id`.
- **The Materialize version** (`spec.environmentdImageRef`) at the time of each
  backup. Restoring a backup starts Materialize at that version.

### Restore role

`restore-blob` restores an S3 blob by deleting its delete marker. That needs
`s3:DeleteObjectVersion`, which can permanently delete data. Don't give it to
Materialize's role. Instead, give it to a separate service account in its own
namespace.

Create an IAM role with this policy. `<prefix>` is the path of
`persist_backend_url`; in the Terraform examples, it is
`system:serviceaccount:<namespace>:<instance name>`.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:DeleteObjectVersion"],
      "Resource": ["arn:aws:s3:::<bucket>/<prefix>/*"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:ListBucketVersions"],
      "Resource": ["arn:aws:s3:::<bucket>"],
      "Condition": {"StringLike": {"s3:prefix": ["<prefix>/*"]}}
    }
  ]
}
```

Let the cluster's OIDC provider assume the role for the subject
`system:serviceaccount:persist-restore:persist-restore`, then create the
namespace and service account:

```shell
kubectl create namespace persist-restore
kubectl label namespace persist-restore pod-security.kubernetes.io/enforce=restricted
kubectl -n persist-restore create serviceaccount persist-restore
kubectl -n persist-restore annotate serviceaccount persist-restore \
  eks.amazonaws.com/role-arn=<restore role ARN>
```

## Restore procedure

Finish each step before starting the next. In particular, Materialize must not
run between restoring the database and restoring the blobs.

```shell
MZ_NAMESPACE=materialize-environment   # the Materialize instance's namespace
MZ_NAME=main                           # the Materialize resource's name
```

### Step 1. Stop Materialize

1. Save the `Materialize` resource, and note its environment ID, image, and
   backend secret name:

   ```shell
   kubectl -n $MZ_NAMESPACE get materialize $MZ_NAME -o yaml > materialize.yaml
   kubectl -n $MZ_NAMESPACE get materialize $MZ_NAME \
     -o jsonpath='{.spec.environmentId}{"\n"}{.spec.environmentdImageRef}{"\n"}{.spec.backendSecretName}{"\n"}'
   ```

   `environmentdImageRef` is the version running now. If you upgraded after
   the backup, it is newer than the backup's version.

1. Delete the `Materialize` resource. This stops environmentd, every cluster
   replica, balancerd, and the console, and leaves the backend secret and the
   data alone:

   ```shell
   kubectl -n $MZ_NAMESPACE delete materialize $MZ_NAME
   ```

   With Terraform, set the `materialize-instance` module's `environment_id` to
   the recorded ID, then destroy only the `Materialize` resource:

   ```shell
   terraform destroy \
     -target='module.materialize_instance.kubectl_manifest.materialize_instance'
   ```

   Don't remove the whole module (for example, with `count = 0`). That also
   deletes the backend secret, the network policies, and possibly the
   namespace.

1. Wait until `kubectl -n $MZ_NAMESPACE get pods` shows no environmentd or
   clusterd pods. Cluster replicas also write to the blob store and consensus.

### Step 2. Restore Metadata database

Managed backups restore a whole server, and except for a Cloud SQL backup
restored in place, they restore into a new server. That works when the server
holds only Materialize's database, as the Terraform `database` modules create
it. If the server is shared with other databases, restore into a new server,
`pg_dump` Materialize's database from it, and load the dump into the original
server as on the `pg_dump` tab.

{{< tabs >}}
{{< tab "pg_dump" >}}

From another database on the same server (for example, `postgres`), recreate
Materialize's database:

```sql
DROP DATABASE materialize WITH (FORCE);
CREATE DATABASE materialize OWNER materialize;
```

Then load the dump as the user that Materialize connects as:

```shell
pg_restore --no-owner --dbname="$METADATA_BACKEND_URL" materialize-<timestamp>.dump
```

{{< /tab >}}
{{< tab "AWS RDS" >}}

List the point-in-time restore window and the snapshots:

```shell
aws rds describe-db-instance-automated-backups --db-instance-identifier <instance> \
  --query 'DBInstanceAutomatedBackups[].RestoreWindow'
aws rds describe-db-snapshots --db-instance-identifier <instance> \
  --query 'DBSnapshots[].[DBSnapshotIdentifier,SnapshotCreateTime,SnapshotType]' \
  --output table
```

Restore into a new instance, to a point in time or from a snapshot. Pass the
source's subnet group, security groups, and parameter group; otherwise RDS uses
the defaults, and Materialize may not be able to reach the new instance.

```shell
aws rds restore-db-instance-to-point-in-time \
  --source-db-instance-identifier <instance> \
  --target-db-instance-identifier <new instance> \
  --restore-time <YYYY-MM-DDTHH:MM:SSZ> \
  --db-subnet-group-name <subnet group> \
  --vpc-security-group-ids <security group> \
  --db-parameter-group-name <parameter group>

aws rds restore-db-instance-from-db-snapshot \
  --db-snapshot-identifier <snapshot> \
  --db-instance-identifier <new instance> \
  --db-subnet-group-name <subnet group> \
  --vpc-security-group-ids <security group> \
  --db-parameter-group-name <parameter group>

aws rds wait db-instance-available --db-instance-identifier <new instance>
aws rds describe-db-instances --db-instance-identifier <new instance> \
  --query 'DBInstances[0].Endpoint.Address' --output text
```

{{< /tab >}}
{{< tab "GCP Cloud SQL" >}}

List the backups, and the latest point you can restore to:

```shell
gcloud sql backups list --instance=<instance>
gcloud sql instances get-latest-recovery-time <instance>
```

Restore to a point in time by cloning into a new instance:

```shell
gcloud sql instances clone <instance> <new instance> \
  --point-in-time=<YYYY-MM-DDTHH:MM:SS.000Z>
gcloud sql instances describe <new instance> --format='value(ipAddresses[0].ipAddress)'
```

Or restore a backup over the existing instance. This replaces every database
on it, and keeps its address, so `metadata_backend_url` doesn't change:

```shell
gcloud sql backups restore <backup ID> --restore-instance=<instance>
```

{{< /tab >}}
{{< tab "Azure Database for PostgreSQL" >}}

List the backups, and the earliest point you can restore to:

```shell
az postgres flexible-server backup list --resource-group <group> --server-name <server> \
  --output table
az postgres flexible-server show --resource-group <group> --name <server> \
  --query backup.earliestRestoreDate
```

Restore to a point in time into a new server. For a server with private
access, pass `--subnet` and `--private-dns-zone` to place the new server where
Materialize can reach it.

```shell
az postgres flexible-server restore --resource-group <group> \
  --name <new server> --source-server <server> \
  --restore-time <YYYY-MM-DDTHH:MM:SSZ>
az postgres flexible-server show --resource-group <group> --name <new server> \
  --query fullyQualifiedDomainName --output tsv
```

{{< /tab >}}
{{< /tabs >}}

If the database moved to a new server, point `metadata_backend_url` in the
backend secret at it before step 3. The user and password are the same as on
the source. With Terraform, update the module inputs that build the URL, and
run only
`terraform apply -target='module.materialize_instance.kubernetes_secret.materialize_backend'`.
A full `terraform apply` would start Materialize too early. The new server
isn't in Terraform's state yet; import it once the restore is done.

The user in `metadata_backend_url` must own the restored tables. If another
user ran the restore, run `REASSIGN OWNED BY <restoring user> TO <Materialize's
user>;` in the restored database.

### Step 3. Restore Blobs

`restore-blob` brings back every blob that consensus references. It ships in
the `materialize/jobs` image.

{{< warning >}}
The `materialize/jobs` version must exactly match the Materialize version that
was running when the backup was taken. That isn't necessarily the version in
`environmentdImageRef` now. An older version can't read the restored data.
{{< /warning >}}

1. Build the consensus URI from `metadata_backend_url`: replace any `options`
   parameter with `options=--search_path=consensus`. For example:

   ```
   postgres://materialize:<password>@<host>:5432/materialize?sslmode=require&options=--search_path=consensus
   ```

1. Store it and `persist_backend_url` in a secret, which keeps the password out
   of the Job spec and your shell history:

   ```shell
   kubectl -n persist-restore create secret generic persist-restore \
     --from-literal=CONSENSUS_URI='<consensus URI>' \
     --from-literal=BLOB_URI='<persist_backend_url>'
   ```

1. Save this Job as `persist-restore.yaml`, with `<version>` set to the
   backup's version. For example, if the backup was taken while
   `materialize/environmentd:v26.1.0` was running, use
   `materialize/jobs:v26.1.0`:

   ```yaml
   apiVersion: batch/v1
   kind: Job
   metadata:
     generateName: persist-restore-
     namespace: persist-restore
   spec:
     backoffLimit: 0
     activeDeadlineSeconds: 3600
     ttlSecondsAfterFinished: 86400
     template:
       spec:
         serviceAccountName: persist-restore
         restartPolicy: Never
         securityContext:
           runAsNonRoot: true
           runAsUser: 65534
           seccompProfile:
             type: RuntimeDefault
         containers:
           - name: persist-restore
             image: materialize/jobs:<version>
             # The image's entrypoint is persistcli. `--commit` belongs to
             # `admin`, so it goes before the subcommand.
             args:
               - admin
               - --commit
               - restore-blob
               - --blob-uri=$(BLOB_URI)
               - --consensus-uri=$(CONSENSUS_URI)
             envFrom:
               - secretRef:
                   name: persist-restore
             resources:
               requests:
                 cpu: "1"
                 memory: 4Gi
               limits:
                 memory: 4Gi
             securityContext:
               allowPrivilegeEscalation: false
               capabilities:
                 drop: ["ALL"]
   ```

   Keep `--commit`. Without it, `restore-blob` fails with `fetching
   just-restored rollup` at the first missing blob, so it doesn't work as a dry
   run.

1. Run the Job and follow its logs:

   ```shell
   JOB=$(kubectl create -f persist-restore.yaml -o name)
   kubectl -n persist-restore wait --for=condition=ready --timeout=10m \
     pod -l batch.kubernetes.io/job-name=${JOB#job.batch/}
   kubectl -n persist-restore logs -f $JOB
   kubectl -n persist-restore get $JOB   # COMPLETIONS 1/1 means it succeeded
   ```

   If it fails with `referenced blobs were not restored`, the backup is
   probably older than the bucket's noncurrent-version expiry, and those blobs
   are gone. Restore a newer backup instead.

1. Delete the secret: `kubectl -n persist-restore delete secret persist-restore`.

### Step 4. Start Materialize

From the saved `materialize.yaml`, remove `status` and the `metadata` fields
that Kubernetes fills in (`uid`, `resourceVersion`, `creationTimestamp`,
`generation`, `managedFields`, and the
`kubectl.kubernetes.io/last-applied-configuration` annotation). Keep
`spec.environmentId`, and set `spec.environmentdImageRef` to the backup's
version. Then apply it:

```shell
kubectl apply -f materialize.yaml
```

With Terraform, set the `materialize-instance` module's `environmentd_version`
to the backup's version, then run `terraform apply`, with `environment_id`
still set.

Upgrade again, if you need to, only after you verify the restore.

The operator names pods and services after the new resource's UID, so the
services come back under new names. Update anything that refers to them by
name, such as an Ingress, a load balancer target, or a port-forward.

### Step 5. Verification

Once environmentd is ready, check that the data from the time of the backup is
there, and that sources are making progress:

```mzsql
SELECT count(*) FROM <a table you know>;
SELECT * FROM mz_internal.mz_source_statuses;
```

## Data loss and upstream systems

- **Writes after the backup are lost**, including table rows and DDL. Rerun any
  DDL you still want.
- **Sources resume from their position at the time of the backup.** A source
  whose upstream still has that position, such as a Kafka topic that still
  holds those offsets, catches up on its own. Others must be recreated, for
  example a PostgreSQL source whose replication slot has moved past it.
- **Sinks may emit some records again**, since they resume from their restored
  progress.
