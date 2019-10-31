# Running materialized on the Linux host, and the rest of the load test in containers.

## Disclaimer

This is hacky, and not intended to be used for any long-term or production use case! A much better way of doing things would be to run a simple DNS server locally that integrates with Docker to map the container names to IP addresses. There are various explanations of how to do this on Stack Overflow.

If you permanently screw up your /etc/hosts somehow, please don't blame me!

I suspect that this will not work at all on macOS.

## Instructions (assuming Ubuntu)

* First make sure you have jq installed:

    sudo sh -c 'yes | apt install jq'

* Edit `ex/chbench/docker-compose.yml` to remove all references to the following containers: `materialize`, `cli`, and all the metrics-related containers (beginning with `grafana` and continuing to the end of the file). Make sure you don't delete the `volumes` section at the bottom.

* From the `ex/chbench` directory, start up all the containers (except the ones you removed):

    sudo docker-compose down && sudo docker-compose up -d --build

* From the top repository directory, start `materialized`:

    cargo run --release --bin materialized

* Add the container hostnames to your hosts file:

    sudo docker network ls | grep chbench_default | awk '{print $1}' | xargs sudo docker inspect | jq -r '.[].Containers | to_entries[] | [.value.IPv4Address, .value.Name] | @tsv' | sed 's|/16||' | sed -E 's/chbench_(.*)_1/\1/' | sudo tee -a /etc/hosts

* Now run the chbench load generator command as normal (see the load_test function in dc.sh for the exact command). With one small difference: When running the `chbench run` command you will need to add the flag `--mz-url='postgresql://172.17.0.1:6875/?sslmode=disable'` to tell the container where to find the host's `materialized` instance.

* **VERY IMPORTANT**: Don't forget to remove all the stuff that got added to `/etc/hosts/` when you are done!!
