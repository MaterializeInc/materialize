# Running with minimal Docker

*Originally authored by Brennan Vincent. Last updated November 26, 2019.*

The guide describes how to run the chbench load test without containerizing
Materialize itself, as the container often interferes with debugging
the `materialized` process (e.g., running `perf`).

## Disclaimer

This is hacky, and not intended to be used for any long-term or production
use case! A much better way of doing things would be to run a simple DNS
server locally that integrates with Docker to map the container names to IP
addresses. There are various explanations of how to do this on Stack Overflow.

If you permanently screw up your /etc/hosts somehow, please don't blame me!

## Instructions

These steps assume Ubuntu, but can be easily adapted for other Linux
distributions or for macOS.

1. First make sure you have jq installed:

   ```shell
   sudo apt install -y jq
   ```

2. Edit `ex/chbench/docker-compose.yml` to remove all references to the
   following containers: `materialize`, `cli`, and all the metrics-related
   containers (beginning with `grafana` and continuing to the end of the file).
   Make sure you don't delete the `volumes` section at the bottom.

1. From the `ex/chbench` directory, start up all the containers (except the ones
   you removed):

   ```shell
   sudo docker-compose down && sudo docker-compose up -d --build
   ```

3. From the top repository directory, start `materialized`:

   ```shell
   cargo run --release --bin materialized
   ```

4. Add the container hostnames to your hosts file:

   ```shell
   sudo docker network ls |
     grep chbench_default |
     awk '{print $1}' |
     xargs sudo docker inspect |
     jq -r '.[].Containers |
     to_entries[] |
     [.value.IPv4Address, .value.Name] |
     @tsv' |
     sed 's|/16||' |
     sed -E 's/chbench_(.*)_1/\1/' |
     sudo tee -a /etc/hosts
   ```

5. Now run the chbench load generator command as normal (see the load_test
   function in dc.sh for the exact command), but with one small difference: when
   running the `chbench run` command you will need to add the flag
   `--mz-url='postgresql://172.17.0.1:6875/?sslmode=disable'` to tell the
   container where to find the host's `materialized` instance.

6. **VERY IMPORTANT**: Don't forget to remove all the stuff that got added to
   `/etc/hosts/` when you are done!
