# Materialize CLI

```shell
Command-line interface for Materialize.

USAGE:
    mz [OPTIONS] <SUBCOMMAND>

OPTIONS:
    -h, --help                 Print help information
    -p, --profile <PROFILE>    Specify a particular configuration profile

SUBCOMMANDS:
    docs       Open the docs
    help       Print this message or the help of the given subcommand(s)
    login      Open the web login
    regions    Show commands for interaction with the region
    shell      Open a SQL shell over a region
```

## Documentation

### Install

Clone the repository and run:

```bash
cargo build --package mz --release
```

After a successful build:

```bash
cd ./target/release
```

**[Install dependencies](#Dependencies)**

### Configuration

Interaction with regions requires a profile. There are two ways to configure one:

- Login command
- Configuration file

The quickest way to create a default profile is by running the login command:

```bash
mz login
```

If you prefer to type your email and password in the console use the `--interactive` option:

```bash
mz login --interactive
```

After login successfully, the CLI will create and populate the configuration file with a default profile.

### Configuration file

The configuration file stores all the available profiles. You can add your own as follows:

```TOML
["profiles.PROFILE_NAME"]
email = "your@email.com"
secret = "YOUR_SECRET"
client_id = "YOUR_CLIENT_ID"
```

##### Paths
Linux: `.config/mz/profiles.toml`
macOS (Monterrey): `.config/mz/profiles.toml`
Windows: `%UserProfile%\.config\mz\profiles.toml`

Example:
```TOML
["profiles.production"]
email = "account@example.com"
secret = "e1620d58-b3f5-454c-bdc8-6ac07da79fd1"
client_id = "f599e025-11b3-4fd8-876f-293787dca3c9"

["profiles.staging"]
email = "account@example.com"
secret = "d0061af2-f4fd-4b31-a5b2-24efac4df25b"
client_id = "c01b79a5-6152-45a3-a8b8-2482bc728458"
```

### Regions

Deploy Materialize in any region:

```bash
mz regions enable aws/us-east-1
```

List all the enabled regions:

```bash
mz regions list
```

Check any enabled region's status:

```bash
mz regions status aws/us-east-1
```

### Shell

Connect to a Materialize region and run your SQL:

```bash
mz shell aws/us-east-1
```

### Help

Use the help command to understand further usage:

```bash
mz help
```

### Dependencies

Install `psql` dependency for the shell command:

```bash

# Linux (TBC)
apt-get install postgresql-client

# macOS
brew install libpq

echo 'export PATH="/usr/local/opt/libpq/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

echo 'export PATH="/usr/local/opt/libpq/bin:$PATH"' >> ~/.bash_profile
source ~/.bash_profile

# Windows (TBC)
```
<<<<<<< HEAD
=======
