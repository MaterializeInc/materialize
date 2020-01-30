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
per month, so the stack is configured to downscale aggressively.

We run two CloudFormation stacks, "buildkite" and "buildkite-builders." At the
time of writing, the builders stack is configured to run a minimum of one agent
and a maximum of two agents, though we may need to adjust this in the future.
Pipelines are written so that the builders stack is used to run `cargo build`,
then immediately farm the resulting binaries out to agents on the other stack.
That way agents in the builder stack have a warm Cargo and Docker cache, and
will typically build much faster because they don't need to recompile all
dependencies.

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

We once used [sccache], a distributed build cache from Mozilla, to share built
artifacts between agents. Unfortunately, for reasons Nikhil never fully tracked
down, setting `RUSTC_WRAPPER=sccache` was causing Cargo to always build from
scratch, a process that was taking 7m+ at the time sccache was removed. Even
back when things were configured correctly, sccache was unable to cache a
number of crates, e.g., crates with a build script could not be cached. The
trouble doesn't seem worth it, unless sccache becomes far more mature.

The current approach is to limit the number of agents that actually build
Rust to the minimum possible. These agents thus wind up with a warm local Cargo
cache (i.e., the cache in the `target` directory, which is quite a bit more
reliable than sccache), and typically only need to rebuild the first-party
crates that have changed.

These agents build a number of Docker images, each with a name starting with
`ci-, that are pushed to Docker Hub. Future steps in the build pipeline run
tests by downloading and orchestrating these Docker images appropriately,
effectively using Docker Hub for intra-build artifact storage. This system
isn't ideal, but it's much faster than having each build step compile its own
binaries.

Note that most of these Docker images don't contain debug symbols, which can
make debugging CI failures quite challenging, as backtraces won't contain
function names, only program counters. (The debug symbols are too large to ship
to Docker Hub; the `ci-test` image would generate several gigabytes of debug
symbols!) Whenever possible, try to reproduce the CI failure locally to get a
real backtrace.

## macOS agent

We run one macOS agent on Buildkite, via [MacStadium], to produce our macOS
binaries. This agent is manually configured. Ask Nikhil for access if you need
it.

The basic configuration is as follows:

```
% brew install --token='<redacted>' buildkite/buildkite/buildkite-agent
% brew services start buildkite-agent
% vim /usr/local/etc/buildkite-agent/buildkite-agent.cfg
<edit to match config above>
% brew install cmake [other materialize dependencies...]
% curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
% cat /usr/local/etc/buildkite-agent/hooks/environment
export AWS_ACCESS_KEY_ID=<redacted>
export AWS_SECRET_ACCESS_KEY=<redacted>
export PATH=$HOME/.cargo/bin:$PATH
```

Be sure to install an SSH key with the appropriate GitHub access in
`~/.ssh/id_rsa`, too.

The agent runs as the default MacStadium `administrator` user, since there isn't
an easy way to isolate builds on macOS.

## Agent security

Unlike builds on a CI platform where hardware is provided, like Travis CI or
Github Actions, our build runners are not hardened against malicious code. This
is problematic for accepting third-party pull requests, which could modify the
build scripts to do all manner of devious things to our infrastructure.

Note that *any* solution that involves running third-party code on our
infrastructure without human validation is unacceptable. We simply do not have
the security expertise in house to have confidence in any sandboxing setup.

The approach taken is to ensure that only "safe" builds are run, where a safe
build is any build that is triggered by a trusted user (i.e., a Materialize
employee or trusted collaborator). A build is safe if:

  * It is triggered from the Buildkite API or UI, as only trusted users have
    access to the Buildkite API or UI.
  * It is triggered by a webhook and:
    * The build branch is an origin branch, as only trusted users can push to
      origin.
    * The build branch is from a fork of the repository, but the fork is owned
      by a GitHub user who is part of the `build-authorized` team.

Unsafe builds are then builds that are automatically triggered by a webhook on a
fork of the repository where the fork's owner is not part of the
`build-authorized` team.

Trusted users should be added to the `build-authorized` team on GitHub and
to the Buildkite organization. Untrusted users must be given access to neither.

These safety checks are performed by a shell script in the `env` file in the
secrets bucket. This `env` file is downloaded and run before every step in every
pipeline on every agent managed by the auto-scaling CloudFormation template.
Keep in mind that standalone agents, like the macOS agent, do not download this
`env` file and therefore should never be used to run untrusted code.

Once a trusted user determines that an untrusted PR contains no malicious code,
the trusted user can press the "Rebuild" button from the Buildkite UI to trigger
a build. This process will need to be repeated whenever new code is pushed to
the PR.

## Deployment

Binary tarballs are deployed to an S3 bucket accessible at
https://downloads.mtrlz.dev. If you're a Materialize developer,
see https://mtrlz.dev for the specific links.

A [ready-to-use materialized image][materialized-docker] is also shipped to
Docker Hub. Unlike the intra-build Docker image, this image contains debug
symbols.

## DNS

The VPC in which agents are launched has a private Route 53 hosted zone,
`internal.mtrlz.dev`. Records are not accessible outside of instances launched
into the VPC.

## Debugging build agents

I propose the following corollary to Murphy's law:

  > Anything that can go wrong will only go wrong in CI.

It follows that one will frequently need to SSH into build agents to diagnose
bugs that only occur in CI. You can even attach GDB to a running Rust test
binary, with a bit of elbow grease.

You'll need the ID of the container in which the faulty process is running and
the exact version of the image it's running. The goal is to launch a new
container with "ptrace" capabilities into the same PID and network namespace,
so that you can attach GDB to the process. If you attempt to GDB directly from
the host, GDB will get confused because userspace differs, and the userspace
libraries that the binary claims to link against won't exist. If you GDB with a
`docker exec` from within the already-running container, GDB will fail to
ptrace, since CI, quite reasonably, doesn't launch containers with ptrace
capabilities.

Roughly:

```bash
# From the host.
$ docker ps
$ docker run -it \
  --pid=container:<CONTAINER-ID> --net=container:<CONTAINER-ID> \
  --cap-add sys_admin --cap-add sys_ptrace \
  --entrypoint bash \
  --rm \
  materialize/test:<YYYYMMDD-HHMMSS>

# Within the container.
$ sudo apt install gdb
$ ps ax | grep <FAULTY-PROCESS-NAME>
$ gdb -p <FAULTY-PROCESS-PID>
```

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
[materialized-docker]: https://hub.docker.com/repository/docker/materialize/materialized
[MacStadium]: https://www.macstadium.com
