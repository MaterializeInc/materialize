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

At the time of writing, our only build agent is a Linux machine,
`mtrlz-office-1`, that Arjun donated to the cause. If it gets stuck, feel free
to log in and give it a prod. It lives under Nikhil's desk. We'll eventually
want to use Buildkite's support for a fleet of AWS build agents that autoscale
in response to the build queue.

SSH access to `mtrlz-office-1` is available. Ask Nikhil to add your account.
Since it doesn't have a static IP, it maintains a reverse SSH tunnel to
mtrlz.dev:2222 via autossh. That means you can connect, credentials permitting,
with something like `ssh USER@mtrlz.dev -p 2222`.

## Buildkite configuration

Note to self: the standard Buildkite configuration, in
/etc/buildkite-agent/buildkite-agent.cfg, sets the `git clean` flags to
`-ffdxq`. This has the annoying effect of deleting gitignored files, like the
Cargo cache in the `target` directory. This increases build isolation at the
(major) cost of requiring a full rebuild. For now I've manually dropped the `-x`
so that repeated builds on the same agent will benefit from a warm cache.

[Buildkite]: https://buildkite.com
[Docker Compose]: https://docs.docker.com/compose/
