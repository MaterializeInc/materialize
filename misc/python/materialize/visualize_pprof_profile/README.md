# Materialize pprof Profile Visualization Tool

This tool allows you to symbolize and visualize pprof profiles offline using debug symbols stored in Materialize's S3 bucket.

## Prerequisites

- Docker installed and running on your system
- AWS credentials with access to the materialize-debuginfo S3 bucket in the Materialize Core account

## Setup

1. Set up your AWS credentials:
```bash
export AWS_PROFILE=<your-profile>
```
Where `<your-profile>` is your AWS profile with access to the materialize-debuginfo S3 bucket in the Materialize Core account.

## Usage

```bash
python3 visualize_pprof_profile.py <path-to-profile> [--port PORT]
```

### Arguments

- `<path-to-profile>`: Path to your pprof.gz profile file (required)
- `--port`: Port number to run the pprof web UI (optional, defaults to 8080)

## How It Works

1. The tool reads your pprof profile and extracts the build ID
2. It automatically fetches the corresponding debug symbols from S3
3. Creates a Docker container with the necessary tools (pprof, graphviz)
4. Starts a web UI where you can analyze the profile

## Important Notes

- The web UI will be available at `http://localhost:<port>` (default: http://localhost:8080)
- Initial symbolization might take a few moments - wait until you see "Serving web UI on http://localhost:8080" message
- The Docker container continues running even after you quit the program
- The profile.proto file is sourced from: https://raw.githubusercontent.com/google/pprof/main/proto/profile.proto

## Example

```bash
# Set up AWS credentials
export AWS_PROFILE=mz-cloud-production-engineering-on-call

# Run the visualization tool
python3 visualize_pprof_profile.py /path/to/your/profile.pprof.gz
```

After running the command, open your web browser and navigate to http://localhost:8080 to view the profile visualization.
