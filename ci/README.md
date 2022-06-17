# CI overview

Our continuous integration provider is [Buildkite]. It works much like Travis or
Circle CI, if you're familiar with either of those providers, except that you
are required to bring your own hardware for the build agents.

Buildkite's model is built around "pipelines", which are independent chains
of commands that can be automatically triggered in response to pushes,
new pull requests, or manual UI actions. We have multiple pipelines in this
repository: the "test" pipeline, for example, runs `cargo test` on every PR,
while the "deploy" pipeline builds the Rust API documentation and publishes
it to dev.materialize.com after a merge to main.

Pipelines are configured with a YAML file that lives in version control,
meaning the build configuration can be versioned alongside the code it builds.
Within this folder, each subdirectory corresponds to one pipeline.

To isolate the build environment, each pipeline runs in a bespoke Docker
container. See [builder/README.md](./builder/README.md) for instructions on
updating the image.

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

Note that most of these Docker images don't contain debug symbols, which can
make debugging CI failures quite challenging, as backtraces won't contain
function names, only program counters. (The debug symbols are too large to ship
to Docker Hub; the `ci-test` image would generate several gigabytes of debug
symbols!) Whenever possible, try to reproduce the CI failure locally to get a
real backtrace.

Build agents sometimes become corrupted and start failing all builds assigned to
them (e.g., because they run out of disk space). If this happens, you can
navigate to the build agent page in the navigation bar, find the affected agent,
and press the "Stop agent" button to terminate the agent. A new agent will spin
up automatically within a minute or two to replace the stopped agent, so feel
free to be aggressive with stopping agents. You can determine which agent ran a
given build job under the "Timeline" tab, listed above the build job's log
output.

## Build caching

We configure [`sccache`](https://github.com/mozilla/sccache) to write
compilation artifacts to an S3 bucket that is shared amongst the build agents.
This makes from-scratch compilation on a fresh agent *much* faster.

## macOS agent

We run two macOS agents on Buildkite, via [MacStadium], to produce our macOS
binaries. These agents are manually configured. Ask Nikhil for access if you
need it.

The basic configuration is as follows:

```shell
% brew install buildkite/buildkite/buildkite-agent
# On M1: /opt/homebrew/etc/buildkite-agent/buildkite-agent.cfg
% vim /usr/local/etc/buildkite-agent/buildkite-agent.cfg
tags="queue=mac,queue=mac-{x86_64|aarch64}"
name="mac-X"
<otherwise edit to match config above>
% brew install cmake postgresql python3 [other materialize dependencies...]
% curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# On M1: /opt/homebrew/etc/buildkite-agent/hooks/environment
% cat /usr/local/etc/buildkite-agent/hooks/environment
export AWS_ACCESS_KEY_ID=<redacted>
export AWS_SECRET_ACCESS_KEY=<redacted>
export PATH=$HOME/.cargo/bin:$PATH
% brew services start buildkite-agent
```

Our goal is to build binaries that will run on the last three macOS versions on
both Intel and ARM machines. This matches the versions that Homebrew supports.
That means building the binaries on a machine running the oldest version of
macOS that we intend to support. (macOS is backwards compatible but not forwards
compatible: binaries built on a newer version of macOS will not run on any older
versions of macOS.) Note that at the moment the earliest version of macOS that
supports ARM is macOS 11, so ARM binaries don't yet work on the three most
recent macOS versions.

The agent runs as the default MacStadium `administrator` user, since there isn't
an easy way to isolate builds on macOS.

The GitHub Actions runner also needs to be installed for CI on
[MaterializeInc/homebrew-crosstools]. Running two separate CI agents on the same
machine is not ideal, as both Buildkite and GitHub Actions might schedule jobs
on the same agent concurrently. But homebrew-crosstools is not active enough to
warrant its own dedicated agent; nor is it easy to run CI for a Homebrew tap on
Buildkite. Homebrew's CI tooling is very tightly coupled to GitHub Actions.

To configure a GitHub Actions agent:

1. Follow the instructions for creating a self-hosted runner in our GitHub
   Enterprise account: https://github.com/enterprises/materializeinc/settings/actions/runners/new

2. Run `./config.sh` and follow the interactive configuration prompts, applying
   the label `macos-x86_64` or `macos-aarch64` depending on the platform.

3. Skip the step that says `run.sh` and run `./svc.sh install && ./svc.sh start`
   instead to install a launchd configuration that automatically starts the
   GitHub Actions agent on boot.

4. On ARM, the service needs to run under Rosetta until [actions/runner#805]
   is resolved. Prefix the calls to `config.sh` and `svc.sh` with `arch -x86_64`
   to accomplish this, as in `arch -x86_64 ./config.sh`.


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
https://binaries.materialize.com. If you're a Materialize developer,
see https://dev.materialize.com for the specific links.

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
bugs that only occur in CI. Unfortunately, per our security policies, only
infrastructure administrators have access to our running CI agents.

What you can do instead is spin up a scratch EC2 instance based on the same AMI
as CI. Run this command from your PR that is failing on CI:

```
$ bin/scratch create <<EOF
{
    "name": "ci-agent-alike",
    "instance_type": "c5.2xlarge",
    "ami": "ami-00abe272e96e4c11e",
    "ami_user": "ec2-user",
    "size_gb": 200
}
EOF
Launched instances:
+-------------------------+---------------------+-------------------+--------------------+-------------------------+---------------------+---------+
|           Name          |     Instance ID     | Public IP Address | Private IP Address |       Launched By       |     Delete After    |  State  |
+-------------------------+---------------------+-------------------+--------------------+-------------------------+---------------------+---------+
| dd5ef155-ci-agent-alike | i-0b03bb914c74f9e69 |     3.17.4.234    |     10.1.27.14     | benesch@materialize.com | 2022-01-03 19:30:23 | running |
+-------------------------+---------------------+-------------------+--------------------+-------------------------+---------------------+---------+
```

<small>* You may need to update the instance type and AMI above as we upgrade our Buildkite agents. The latest Buildkite AMI is available [here][elastic-yml] under
the `AWSRegion2AMI` heading. Use the AMI for `us-east-2` and `linuxamd64`.</small>

This will create an EC2 instance that looks like a CI agent and push your local
copy of the repository to it. You can SSH in to the agent using the instance ID
printed by the previous command and run the CI job that is failing.

Every CI job is a combination of an mzcompose "composition" and a "workflow". A
composition is the name of a directory containing an mzcompose.yml or
mzcompose.py file. A workflow is the name of a service or Python function to run
within the composition. You can see the definition of each CI job in
[ci/test/pipeline.template.yml](./test/pipeline.template.yml). To invoke a
workflow manually, you run `bin/mzcompose --find COMPOSITION run WORKFLOW`.

For example, here's how you'd run the testdrive job on the EC2 instance:

```
bin/scratch ssh INSTANCE-ID
cd materialize
bin/mzcompose --find testdrive run default
```

If the test fails like it did in CI, you're set! You now have a reliable way to
reproduce the problem. When you're done debugging, be sure to spin down the
instance:

```
bin/scratch destroy INSTANCE-ID
```

### Serious heisenbugs

If you're unable to reproduce the bug on a CI-alike agent, you'll need to enlist
the help of someone on the infrastructure team who can SSH into an actual
running CI agent.

You can even attach GDB to a running Rust test binary, with a bit of elbow
grease.

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

[autouseradd]: https://github.com/benesch/autouseradd
[Buildkite]: https://buildkite.com
[Docker Compose]: https://docs.docker.com/compose/
[MacStadium]: https://www.macstadium.com
[materialized-docker]: https://hub.docker.com/repository/docker/materialize/materialized
[MaterializeInc/homebrew-crosstools]: https://github.com/MaterializeInc/homebrew-crosstools
[materializer GitHub user]: https://github.com/materializer
[actions/runner#805]: https://github.com/actions/runner/issues/805
[elastic-yml]: https://s3.amazonaws.com/buildkite-aws-stack/latest/aws-stack.yml
