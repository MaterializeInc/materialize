# Materialize Console Docker Setup

This document describes how to build and run the Materialize Console Docker image.

## Building the Docker Image Locally

To build the Docker image locally, run the following command from the `console/` directory:

```bash
docker build -f misc/docker/Dockerfile -t console-dev:latest -t console-dev:1.0.0 .
```

This will build the image from the `misc/docker/Dockerfile` and tag it as `console-dev:latest` and `console-dev:1.0.0`. You can replace `1.0.0` with any version you want to use.

## Running the Container

After building the image, you can run the container with the following command:

```bash
docker run --rm -p 3000:8080 \
    -e MZ_ENDPOINT="http://materialized:6876" \
    --name console-container \
    --health-interval=30s \
    console-dev:latest
```

and access the Console through `http://localhost:3000/`.

> Note: The `MZ_ENDPOINT` environment variable should point to your running Materialize instance. If you are running it locally, make sure that Materialize is exposed on port 6876 and accessible from the Docker network. For macOS, you can use `http://host.docker.internal:6876` as the endpoint.

## Running with the Materialize Emulator (Recommended)

The Console comes embedded with the Materialize Emulator. To build and run the emulator, follow [Installing the emulator](https://materialize.com/docs/get-started/install-materialize-emulator/):

## Local development

If you need to run the developer build of the `console` with hot reloading, running `CONSOLE_DEPLOYMENT_MODE='flexible-deployment' yarn start` will automatically proxy all http/ws Materialize API requests to `localhost:6876` by default, which should be the Materialize instance. If you need to point to another localhost port, you can specify the variable `DEV_SERVER_PROXY_PORT`. The Console will still be accessible through `http://localhost:3000/`.

### Pointing at a local kind setup

Once a Materialize instance is set up in Kind, you can port forward balancerd via `kubectl` to some port. Below, I've forwarded balancerd's internal port, 6876, to 8080:

```
kubectl port-forward svc/mzesy3v0nwmn-balancerd 8080:6876 -n materialize-environment
```

<sub>Note: You can also port-forward the Kind instance's Console service since all requests through it get proxied to balancerd via nginx</sub>

You can then run the following command to point Console at the Kind Materialize instance:

```
CONSOLE_DEPLOYMENT_MODE='flexible-deployment' DEV_SERVER_PROXY_PORT=8080 yarn start
```

### TLS

If the Materialize instance uses TLS, you'll need to run `CONSOLE_DEPLOYMENT_MODE='flexible-deployment' DEV_SERVER_WITH_TLS_PROXY='true' yarn start`.

## Nginx Configuration

The Nginx server is pre-configured to serve static assets and proxy `/api` requests to the Materialize endpoint.

### Modifying Nginx Configuration

If needed, update the configuration file located at:

```
misc/docker/nginx.conf.template
```

### Health Check

The container includes a health check that verifies the Nginx server is running:

```bash
HEALTHCHECK --interval=30s --timeout=3s \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/ || exit 1
```
