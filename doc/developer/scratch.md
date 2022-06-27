# "scratch" infrastructure

## Overview
Materialize provides employees with on-demand infrastructure in an
AWS account called "scratch". This account is intended for quick and dirty tests,
benchmarks, short-lived side projects, and so on.

Scratch is *not* intended for long-term or mission-critical infrastructure, which we manage in
the [i2 repo](https://github.com/MaterializeInc/i2). Machines created in scratch are
therefore automatically deleted after about a week.

## Permissions
All members of the engineering org should have full administrative control of
the account; if you don't have access and believe you should, check with your onboarding buddy or manager.

If you are a member of a different org but feel that access to AWS resources could
be useful to you, ask in #eng-infra on Slack.

To access scratch resources from the AWS console, use [our SSO app](https://materialize.awsapps.com/start/).

To access scratch resources from the command line, follow the instructions [here](https://github.com/MaterializeInc/i2/blob/main/doc/aws-access.md#cli-and-api-access).

## Management with the `bin/scratch` script

The `bin/scratch` script is intended to make it easy to launch EC2 instances from the command line. It has three
subcommands: `create`, `mine`, and `destroy`.

### `bin/scratch create`

This subcommand expects a series of JSON objects on standard input, each of which describes a machine to launch and configure in EC2. An example follows:

```
{
    "name": "chbench",
    "launch_script": "bin/mzcompose --find chbench run cloud-load-test",
    "instance_type": "r5a.4xlarge",
    "ami": "ami-0aeb7c931a5a61206",
    "size_gb": 200,
    "tags": {
        "scrape_benchmark_numbers": "true",
        "lt_name": "release-chbench",
        "purpose": "load_test",
        "mzconduct_workflow": "cloud-load-test",
        "test": "chbench",
        "environment": "scratch"
    }
}
```

See [Grafana Integration](#grafana-integration), below, for some details about the tags.

`bin/scratch create` takes in configs from stdin, or by passing a name as a positional arg, like:

```
$ bin/scratch create dev-box
```
It looks for the name as a json file in `misc/scratch`, a good starter to just do some plain, personal testing is `dev-box`.

---

All of the keys are required (though `tags` can be an empty dictionary). Their meanings should largely be self-explanatory.

Any number of JSON objects may be specified, one after the other. The script creates a "cluster" of machines identified by a random
6-digit hexadecimal nonce. The `materialize` repo is pushed to each machine in the cluster, and then the specified launch script
is run in the background. After kicking off the launch script (but without waiting for it to complete), the script terminates.

Each cluster's `/etc/hosts` file is modified to point to the other members of the cluster by name; for example, from another machine
the operator could ping the above-described machine with `ping chbench`.

#### SSH access

SSH access to the instances is provided by [EC2 instance connect]. If you
specify a custom AMI, you need to make sure it's an AMI that supports EC2
instance connect.

To SSH to an instance:

```
bin/scratch ssh INSTANCE-ID
```

If you need to, you can install and use the `mssh` command provided by the
underlying [EC2 connect CLI] directly, but it's usually much easier to go
through `bin/scratch ssh`.

To use Visual Studio Code Remote development with the instance, you need to install an SSH key on it.
Use `bin/scratch ssh INSTANCE-ID`, then append to the `.ssh/authorized_keys` file your public SSH key (should be a local file like `~/.ssh/something.pub`).
Then use the [guide](https://code.visualstudio.com/docs/remote/ssh) to connect with VS Code.

### `bin/scratch mine`

This subcommand lists all the machines that a given user has created with `bin/scratch create`, along with metadata about them. For example:

```
brennan@New-Orleans ~ ❯❯❯ ~/code/materialize/bin/scratch mine
+-------------------+---------------------+-------------------+--------------------+-------------------------+---------------------+---------+
|        Name       |     Instance ID     | Public IP Address | Private IP Address |       Launched By       |     Delete After    |  State  |
+-------------------+---------------------+-------------------+--------------------+-------------------------+---------------------+---------+
| f2b1bc7a-btv_test | i-02790a4efb77b06b4 |   18.191.163.58   |    10.1.26.167     | brennan@materialize.com | 2021-09-16 12:39:50 | running |
+-------------------+---------------------+-------------------+--------------------+-------------------------+---------------------+---------+
```

Important options include `--all`, which lists machines for all users, and `--output-format csv`, which does what it looks like. To look up
machines for someone other than yourself, list their email addresses at the end of the command, like so: `scratch mine eli@materialize.com`.

### `bin/scratch push`

```
bin/scratch push <instance_id>
```

`push` re-pushes your git `HEAD` to the specified instance. You can override the commit to checkout
with `--rev`

### `bin/scratch destroy`

This subcommand terminates a list of machines given by Instance ID on the command line. For example:

```
bin/scratch destroy i-02790a4efb77b06b4
```

As a convenience, you can also destroy all of your instances with the
`--all-mine` option:

```
bin/scratch destroy --all-mine
```

Pass `--dry-run` if you want to see what instances `bin/scratch destroy` would
destroy without actually destroyin them.

### Grafana integration

The `environmentd` process always exposes metrics at its primary port's HTTP server on the
prometheus-standard `/metrics` path, but the scratch instance needs to be configured correctly for
our Prometheus server to actually scrape the metrics and thereby expose them to Grafana.

The tl;dr is that you must configure mzcompose with `--preserve-ports` and the EC2 instance with
the `"scrape_benchmark_numbers": "true"` tag. Thus a bare-minimum Grafana-integrated config looks
like:

```javascript
{
    "launch_script": "bin/mzcompose --preserve-ports ..<remainder of args>"
    // .. snip config ..
    "tags": {
        "scrape_benchmark_numbers": "true"
    }
}
```

Read on for more details and some other items that can be configured.

#### Prometheus config

There are three tags on the EC2 instance that configure our Prometheus integration:

* `scrape_benchmark_numbers`: must be set to exactly the string `"true"` in order for Prometheus
  to observe the instance.
* `purpose`: is used as a filter in the Grafana UI. You can use this to group all your instances
  (e.g. set it to `myname-debugging`) or set it to `load-test` or `benchmark`.
* `test` displays as an additional filter inside of the Grafana UI.

So the minimum scratch config to get metrics into Grafana looks like:

```javascript
{
    // .. snip general config ..
    "tags": {
        "scrape_benchmark_numbers": "true"
    }
}
```

And a slightly more complete one could be:

```javascript
{
    // .. snip general config ..
    "tags": {
        "scrape_benchmark_numbers": "true",
        "purpose": "bwm-debugging",
        "test": "chbench"
    }
}

{
    // .. snip general config ..
    "tags": {
        "scrape_benchmark_numbers": "true",
        "purpose": "bwm-debugging",
        "test": "billing"
    }
}
```

#### mzcompose config

Prometheus only looks for Materialize metrics on port 6875. The canonical way to ensure that
Materialize is available on port 6875 on a host is to pass the `--preserve-ports` argument to
mzcompose. (Without this flag, mzcompose chooses a random host port for Materialize, which
will be unknown to Prometheus.)

```javascript
{
    "launch_script": "bin/mzcompose --preserve-ports ..<remainder of args>"
    // .. snip config ..
}
```

[EC2 instance connect]: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Connect-using-EC2-Instance-Connect.html
[ec2instanceconnectcli]: https://github.com/aws/aws-ec2-instance-connect-cli
