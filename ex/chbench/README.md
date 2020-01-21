# chbench

This is a demonstration of Materialize on [CH-benCHmark]—a mashup of TPC-C and
TPC-H designed to test the speed of analytics queries on a rapidly changing
dataset.

There are several moving pieces to this demo. At the bottom of the stack, we
have a MySQL instance that stores the TPC-C dataset. We connect the
CH-benCHmark's transactional load generator to this MySQL instance, sending a
configurable volume of new orders and such through MySQL. Then, we pipe the
MySQL binlog into Kafka (via Debezium and Kafka Connect, though the details are
not so important), and plug `materialized` into the other end. Then we
install the TPC-H queries into `materialized` as materialized views, and watch
as they magically stay up to date.

The components are orchestrated via [Docker Compose][docker-compose], which
runs each component in a Docker container. You can run this demo via Linux,
an EC2 VM instance, or a Mac laptop. Note that running Docker Compose will cause
some overhead on macOS; to measure performance, you'll want to use Linux.

Should you want to run this demo on a Mac laptop, you'll
want to increase memory available to Docker Engine using the following steps:
   1. Open Docker for Mac's preferences window
   2. Go to the "Advanced" section.
   3. Slide the "Memory" slider to at least 8 GiB.
   4. Click "Apply and Restart".
   5. Continue with the `docker-compose` steps listed above.

To get started, bring up the Docker Compose containers in the background. To do this, open up a new shell, and from the Materialize repository, change to the `ex/chbench` directory and type the following three commands after each other:

```shell session
$ docker-compose up -d --build
Creating network "chbench_default" with the default driver
Creating chbench_inspect_1      ... done
Creating chbench_chbench_1      ... done
Creating chbench_cli_1          ... done
Creating chbench_mysql_1        ... done
Creating chbench_materialized_1 ... done
Creating chbench_connector_1    ... done
Creating chbench_zookeeper_1    ... done
Creating chbench_kafka_1        ... done
Creating chbench_connect_1         ... done
Creating chbench_schema-registry_1 ... done
```

If all goes well, you'll have MySQL, ZooKeeper, Kafka, Kafka Connect, and
Materialize running, each in their own container, with Debezium configured to
ship changes from MySQL into Kafka. You won't really be able to tell if things
fail to start up, which seems to happen from time to time, as the output from
Docker Compose only tells you if the *container* successfully booted, not if the
service inside the container actually booted. Your best bet is to assume that
the services came up and proceed with the demo—the logs are too spammy to be
useful in quickly determining whether the service came up successfully.

Now, generate some CH-benCHmark data. You'll only need to do this step once, as
the generated data is stored on a Docker volume that persists until you manually
remove it with `docker volume rm`, so if this is your second time through the
demo, you can skip this step.

```shell session
$ docker-compose run chbench-init
$ docker-compose run chbench gen --warehouses=1
```

You can generate bigger datasets by using more warehouses. Once the data is
generated, it's time to fire up some load! Type the follow command, which will
take control of your session and run for the specified number of seconds.

```shell session
$ docker-compose run chbench run \
    --dsn=mysql --gen-dir=/var/lib/mysql-files \
    --analytic-threads=0 --transactional-threads=5 --run-seconds=300
Databasesystem:
-initializing
Schema creation:
-succeeded
CSV import:
-succeeded
-check with 1 warehouses succeeded
Additional Preparation:
-succeeded
Wait for threads to initialize:
-all threads initialized
Workload:
-start warmup
-start test
```

Once you see that the CSV import has succeeded, the initial data set has been
loaded into MySQL. The load generator will continue to run for the specified
duration.

It's time to connect to `materialized` and install some materialized views! In
a new shell, in the same directory, type the following which will bring up a
materialize shell.

```shell session
$ docker-compose run cli
CREATE SOURCES LIKE 'mysql.tpcch.%' FROM 'kafka://kafka:9092' USING SCHEMA REGISTRY 'http://schema-registry:8081';

CREATE VIEW q01 as SELECT
        ol_number,
        sum(ol_quantity) as sum_qty,
        sum(ol_amount) as sum_amount,
        avg(ol_quantity) as avg_qty,
        avg(ol_amount) as avg_amount,
        count(*) as count_order
FROM
        mysql_tpcch_orderline
WHERE
        ol_delivery_d > date '1998-12-01'
GROUP BY
        ol_number;

PEEK q01;
PEEK q01;
PEEK q01;
```

For maximum style points, use `watch-sql` for some live query monitoring:

```bash session
$ docker-compose run cli watch-sql "PEEK q01"
```

To ensure that `materialized` reflects all changes to the source data, you can
run:

```bash session
$ docker-compose run schema-registry flush-tables
```

[CH-benCHmark]: https://db.in.tum.de/research/projects/CHbenCHmark/index.shtml?lang=en
[docker-compose]: https://docs.docker.com/compose/

## Using the MySQL CLI

If you want to access a MySQL shell, run the following in the
`ex/chbench` directory:

```
docker-compose run mysqlcli
```

If you've just run `docker-compose up`, you might need to wait a few seconds
before running this.

## Viewing metrics

There are several services that can be used to see how materialize is running. Our custom
system is via grafana, and when you run `docker-compose up` you will get grafana
listening on port 3000.

To view metrics, just visit: http://localhost:3000/d/mz

If you want to be able to edit the dashboard you will need to log in:
http://localhost:3000/login the username/password is admin/admin.

If you don't save the dashboard then **reloading the page will destroy your edits**.
Click the save floppy disk icon and copy the resulting JSON into
`grafana/dashboards/materialize.json`.

## Running with less Docker

Docker can get in the way of debugging materialized—for example, it makes
running `perf` on the materialized binary challenging. There are two easy ways
around this:

  * Running the `materialized` process outside of Docker, as described in
    ["Running with minimal Docker"](docker-local.md).
  * Using the [Nix test harness][nix] in the mtrlz-setup repository.

[nix]: https://github.com/MaterializeInc/mtrlz-setup/tree/master/nix

## Running on AWS EC2

chbench can be run semi-automatically on AWS EC2 with the help of [Terraform],
a tool which manages cloud infrastructure. If you're unfamiliar with Terraform,
you may want to read the [Introduction to Terraform] first.

To install Terraform with Homebrew on macOS:

```shell
brew install terraform
```

You'll also need to ensure the VM that gets created on your behalf will have
access to clone the Materialize repository and download the Materialize Docker
images. These credentials are pulled from your host machine once you've
performed the folowing steps:

  1. Log in to Docker (via `docker login`) on your host machine. These
     credentials will be automatically propagated to the VM.

  2. Ensure your SSH agent has an SSH key installed that can be used to clone
     the Materialize repository. Usually `ssh-add` is sufficient, but if you
     don't have an SSH agent running, you'll need to run `$(eval ssh-agent)`
     first. SSH agent forwarding will be used to clone the repository on the VM.

  3. Make your membership in the MaterializeInc GitHub organization public. See
     ["Publicizing or hiding organization membership"][org-membership] for
     details. All public GitHub keys associated with the MaterializeInc GitHub
     organization will have permission to log in to the VM. If your membership
     isn't public, you won't be able to SSH into the VM!

  4. Store your AWS credentials in `~/.aws/credentials`. Refer to the [AWS
     documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html)
     for instructions on how to manage and retrieve your AWS credentials. Your
     `credentials` file should look like this:

     ```ini
     [default]
     aws_access_key_id=[access key ID goes here]
     aws_secret_access_key=[secret access key goes here]
     region=us-east-2
     ```

Finally, we're ready to go change into the `terraform` directory and run
`terraform apply`:

```shell
cd ex/chbench/terraform
terraform init # this is only needed the first time
terraform apply
```

If all goes well, you'll see output like this:

```
Apply complete! Resources: 1 added, 0 changed, 0 destroyed.

Outputs:

instance_id = i-0a825833f82835dd6
instance_ip = 18.222.150.159
instance_username = ubuntu
ssh_command = ssh ubuntu@18.222.150.159
```

You should now be able to run the displayed SSH command to access your instance:

```
$ ssh ubuntu@18.222.150.159
Welcome to Ubuntu 18.04.3 LTS (GNU/Linux 4.15.0-1051-aws x86_64)

 * Documentation:  https://help.ubuntu.com
 * Management:     https://landscape.canonical.com
 * Support:        https://ubuntu.com/advantage

---- 8< ----
```

If you get a permission denied error or SSH timeout, you probably forgot the
step about making public your membership in the MaterializeInc GitHub
organization. Toggle that bit and wait a few minutes, and you should have access
to the VM. (The VM automatically updates the list of authorized users every two
minutes.) You should also check that your GitHub account lists the public half
of the SSH key on your machine.

Once logged in, you can use `dc.sh` or `docker-compose` as described above to
run chbench.

[Terraform]: https://www.terraform.io
[Introduction to Terraform]: https://www.terraform.io/intro/index.html
[org-membership]: https://help.github.com/en/github/setting-up-and-managing-your-github-user-account/publicizing-or-hiding-organization-membership


## Updating the EC2 AMI

The EC2 provisioning relies on a custom AMI (Amazon Machine Image) that has
Docker and Docker Compose pre-installed, as well as a swapdisk configured.
This AMI can be built automatically via [Packer].

To install Packer with Homebrew on macOS:

```shell
brew install packer
```

Then, from this directory, run Packer:

```shell
cd ex/chbench
packer build packer.json
```

The output will include the ID of the new AMI. If you'd like to switch the
Terraform configuration to use this new AMI, update the `ami` line in
[terraform/main.tf](terraform/main.tf) appropriately. If the AMI is unusable,
you should deregister it, via the online AWS console, to avoid being billed for
storage.

**Note**: the generated AMI will be public! Do not include anything sensitive in
the AMI itself. Use an additional Terraform provisioning step to download or
install sensitive information, like we do with the Docker credentials and the
Materialize repository.

[Packer]: https://www.packer.io
