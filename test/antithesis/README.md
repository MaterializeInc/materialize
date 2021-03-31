# Antithesis testing integration

To deploy to Antithesis:

1. Run `docker login` with the appropriate credentials for Antithesis.
2. Run `./deploy.sh`.

This only works on Linux at the moment.

To modify the workload, change `driver/docker-compose.yml`. If you add or remove
images from the configuration, be sure to update
`misc/python/cli/deploy_antithesis.py` accordingly.
