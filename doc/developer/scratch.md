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
    "launch_script": "MZ_WORKERS=4 bin/mzcompose --mz-find chbench run cloud-load-test",
    "instance_type": "r5a.4xlarge",
    "ami": "ami-0b29b6e62f2343b46",
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

All of the keys are required (though `tags` can be an empty dictionary). Their meanings should largely be self-explanatory.

Any number of JSON objects may be specified, one after the other. The script creates a "cluster" of machines identified by a random
6-digit hexadecimal nonce. The `materialize` repo is pushed to each machine in the cluster, and then the specified launch script
is run in the background. After kicking off the launch script (but without waiting for it to complete), the script terminates.

Each cluster's `/etc/hosts` file is modified to point to the other members of the cluster by name; for example, from another machine
the operator could ping the above-described machine with `ping chbench`.

#### Access

By default, `bin/scratch` arranges machines for SSM access. Developers may use the AWS SSM command-line tool to access machines given their instance IDs. For example:

```
AWS_PROFILE=mz-scratch-admin AWS_DEFAULT_REGION=us-east-2 aws ssm start-session --target   i-064432ea480ef7e10
```

Once the remote shell is opened, run `sudo su - ubuntu` to access the main (`ubuntu`) account on the host.

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

### `bin/scratch destroy`

This subcommand terminates a list of machines given by Instance ID on the command line. For example:

```
bin/scratch destroy i-02790a4efb77b06b4
```
