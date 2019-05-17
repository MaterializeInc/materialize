# CI overview

Our continuous integration provider is [Buildkite]. It works much like Travis or
Circle CI, if you're familiar with either of those providers, except that you
are required to bring your own hardware for the build agents.

Buildkite's model is built around "pipelines", which are independent chains
of commands that can be automatically triggered in response to pushes,
new pull requests, or manual UI actions. We have multiple pipelines in this
repository: the "test" pipeline, for example, runs `cargo test` on every PR,
while the "deploy" pipeline builds the Rust API documentation and publishes
it to mtrlz.dev after a merge to master.

Pipelines are configured with a YAML file that lives in version control,
meaning the build configuration can be versioned alongside the code it builds.
Within this folder, each subdirectory corresponds to one pipeline.

To isolate the build environment, each pipeline runs in a bespoke Docker
container. Modifying a pipeline's build environment is a three step process.
First, adjust the appropriate Dockerfile. Second, run `bin/ci-image push
PIPELINE` to build a new Docker image, tag it with the current date and time,
and push it to Docker Hub. You'll need Docker credentials for this step.
Finally, update the image configuration in the pipeline.yml configuration file
with the new image tag.

Note that more complicated pipelines use [Docker Compose] to manage spinning up
other services, like ZooKeeper.

## Build agents

Buildkite ships a CloudFormation template that automatically scales build agent
instances on AWS, called the "Elastic CI Stack". The best source of information
is the documentation in the repository's README, at
<https://github.com/buildkite/elastic-ci-stack-for-aws>.

At the time of writing, we use EC2's c5.2xlarge instances, which have 8 vCPUs
(i.e., hyperthreads) and 16GB of memory. These instances cost approximately $250
per month, so the stack is configured to downscale aggressively. We leave one
instance running at all times, however, to minimize latency; this agent will
have a warm Cargo and Docker cache, and so will typically be much faster at
running builds.

TODO(benesch): we should hook up sccache with a distributed storage engine,
so that fresh build agents don't need to recompile the world from scratch.

TODO(benesch): we could consider warming the Docker cache, so that fresh
build agents don't need to waste time downloading the latest Docker images.

## Buildkite configuration

Configuration of the CI stack lives in three places: the CloudFormation
template, which contains the bulk of the knobs; the "buildkite-managedsecrets"
S3 bucket, which contains SSH keys and Docker credentials; and the
"buildkite-config" S3 bucket, which contains a bootstrapping script to tweak the
agent configuration.

If the CloudFormation template gets lost, the important configuration knobs are
listed below, along with their rationale.

Parameter name                     | Value
---------------------------------- | -----
**InstanceType**                   | `c5.2xlarge`
**ManagedPolicyARN**               | `arn:aws:iam::000000000000:policy/buildkite-agent`<br><br>An IAM policy that permits access to the bootstrap script in the buildkite-config S3 bucket. This is documented in detail in the official CI stack documentation.
**KeyName**                        | The name of an EC2 SSH key that will permit you to log into the build agents, should you need to diagnose a problem.
**BootstrapScriptUrl**             | `s3://buildkite-config-<SECRETNAME>/bootstrap.sh`
**EnableDockerUserNamespaceRemap** | `false`<br><br>User namespace remapping breaks permissions on bind mounts, as it interacts poorly with [autouseradd]. It's possible that user namespace remapping could be used in place of autouseradd, but that requires some investigation, and it's not like we need the additional security of user namespace remapping, as we're not currently running untrusted PRs in CI.

The current bootstrap script looks like this:

```bash
#!/usr/bin/env bash

# Create cache directories that are owned by the buildkite-agent user,
# otherwise the Docker daemon will create them as root, making them
# unavailable from within the container.
for dir in /var/lib/buildkite-agent/{.cache/sccache,.cargo}
do
    sudo mkdir -p "$dir"
    sudo chown -R buildkite-agent: "$dir"
done

# Don't clean gitignored files. This allows reuse of the Rust build
# cache, which is stored in the ignored "target" directory.
cat <<EOF >> /etc/buildkite-agent/buildkite-agent.cfg
git-clean-flags=-ffdq
EOF
```

**Warning:** this bootstrap script is likely to diverge from the source of truth
in the S3 bucket. If you update the bootstrap script, download the current
version from S3, update it, and reupload it. Don't use the version here as your
base.

The secrets in the buildkite-managedsecrets bucket are, for obvious reasons, not
reproduced here. If you need to recreate the SSH key:

  1. Create a new SSH key with `ssh-keygen`.
  2. Log in as the [materializer GitHub user] and add the new SSH key. This
     grants read/write access to all private Materialize GitHub repositories.
     The credentials for the materializer account are in the company 1Password.
  3. Log into mtrlz.dev and add the SSH key to the buildkite user's
     authorized_keys file.

If you need the Docker hub password, it's in the company 1Password.

## Build caching

Rust compilation is slow enough that we use [sccache], a distributed build cache
from Mozilla, to share a build cache between agents. This dramatically speeds up
the first build on a new agent.

At the time of writing, the build cache is stored in a memcached instance
managed by ElastiCache. From a Buildkite agent, the memcached instance is
available at `buildcache.internal.mtrlz.dev:11211`.

## DNS

The VPC in which agents are launched has a private Route 53 hosted zone,
`internal.mtrlz.dev`. Records are not accessible outside of instances launched
into the VPC.

## Performance testing

We have a dedicated Linux machine in the office that Nikhil intends to use for
reproducable performance testing. (Cloud VMs tend to be very noisy.) To get SSH
access, ask Nikhil to add your account. We don't currently pay WeWork for a
static IP, so we maintain a reverse SSH tunnel to mtrlz.dev:2222 via autossh.
That means you can connect, credentials permitting, with something like `ssh
USER@mtrlz.dev -p 2222`.

[Buildkite]: https://buildkite.com
[Docker Compose]: https://docs.docker.com/compose/
[autouseradd]: https://github.com/benesch/autouseradd
[materializer GitHub user]: https://github.com/materializer
