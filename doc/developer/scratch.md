# Scratch script

## Overview
Materialize includes a script at `bin/scratch` for launching AWS assets (currently, EC2 instances) in the "Scratch" account.

The intention of the script is to create short-lived infrastructure for load tests, benchmarks, experimentation, and similar purposes.

## Caveats
All assets are cleaned up after one week; as such, the Scratch account should not be used for long-lived infrastructure.

The Scratch account is not intended as a secure enclave: the instances are open to TCP connections on all ports, and all engineers at Materialize have full administrative access to the AWS account. These properties may change in the future, but the overall point remains: do not use the Scratch account for work which requires confidentiality or high security.

## Description

`bin/scratch` is controlled by a variety of command-line arguments determining the properties of the cluster of EC2 instances it spins up; the properties of the individual machines in the cluster are read from a series of concatenated JSON objects on standard input.

Relevant environment variables are `AWS_PROFILE` and `AWS_DEFAULT_REGION`, these are set to `mz-scratch-admin` and `us-east-2` by default.

Materialize employees should not normally need to use command-line arguments or environment variables, as those have sane defaults for running things in our own infrastructure. For outside contributors, some changes may be necessary; run `bin/scratch -h` for a full list of options.

All launched machines automatically include the full Materialize source code (at the current commit), along with a basic set of software necessary for running Materialize load tests (for example, Docker).

Shell access to the machines are granted via SSM to all employees who have access to the scratch account.

The JSON objects describing individual machines in the cluster have the following keys, as of this writing:

* **name**: Controls part of the name of the machine in EC2, which takes the form `<cluster_wide_nonce>-<name>`.
* **launch_script**: A shell snippet that will be run after the machine is launched.
* **instance_type**: The AWS instance type; for example, `r5ad.4xlarge`.
* **ami**: The AWS AMI to use. The setup scripts assume that this is an Ubuntu-like machine on 18.04 or later.
* **size_gb**: The available storage space of the root disk, in gigabytes
* **tags**: A dictionary of additional tags to apply to the machine.

## Initial login

Materialize employees will need to log in to our scratch AWS account, via SSO, in order to spin up load tests.

To do so using your web browser, run:

```
aws sso login --profile mz-scratch-admin
```

To do so without access to a web browser (e.g., on a non-graphical shell):

```
BROWSER=true aws sso login --profile mz-scratch-admin
```

This will need to be repeated periodically, as the credentials expire every 12 hours.

## Example

To run our suite of release load tests, log in as described above, and then run:

```
bin/scratch < misc/load-tests/release.json
```
