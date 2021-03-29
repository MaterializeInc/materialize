# fuzz testing support

"ATP" is the codename for our new fuzz testing platform.

To deploy to ATP, run:

```
./deploy.sh
```

To modify the workfload, change `driver/docker-compose.yml`. If you add or
remove images to the configuration, be sure to update
`misc/python/cli/deploy_atp.py` accordingly.
