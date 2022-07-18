# Materialize CLI [WIP]

Rust implementation of critical features for usage and deployment of Materialize platform instances.
It provides the following actions:

* Login
* Enable/Delete Regions
* Shell

## Documentation

### Install

Clone the repository. Once inside, run:

```bash
cargo build --package mz --release
```

After a successful build:

```bash
cd ./target/release
```

[Install dependencies](#Dependencies)

### Help

Use command help to understand usage:

```bash
mz help
```

### Login

The CLI lets you login into the platform and automatically create all it needs to work.

Using the browser:

```bash
mz login
```

Using email and password:

```bash
mz login interactive
```

### Profiles

The CLI uses `USER_ID`, `SECRET`, and `Email` to interact with the platform. These values are automatically populated through the login command. If you want to retrieve or manually create the profiles file, the file is in the OS config folder:

Linux: `.config/mz/profiles.toml`
macOS (Monterrey): `.config/mz/profiles.toml`
Windows: (TBC)


### Regions

Deploy Materialize in any region by simply:

```bash
mz regions enable us-east-1
```

Delete any region using:

```bash
mz regions delete us-east-1
```

***Delete command is followed by a prompt warning to avoid any accidental delete***


### Shell

Connect to a Materialize instance to run your SQL using the CLI shell:

```bash
mz shell us-east-1
```

### Dependencies

Install `psql` dependency for the shell:

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


### Future work
#### Improvements
- [ ]  Handle multiple profiles in .toml
- [ ]  Make error messages standard
- [ ]  Selecting a default region
- [ ]  Run requests in parallel
- [ ]  Enable selecting an environment as a flag in `Shell`
- [ ]  `region` should be a flag not a subcommand
- [ ]  `Interactive` should be a flag and not a subcommand
- [ ]  Reduce `.unwrap` usage
- [ ]  Add optional `—force` command to delete to avoid manual input
- [ ]  Redirect to Materialize after success token saving
- [ ]  Handle login and there is a profile created?
- [ ]  Handle request errors to frontegg
- [ ]  Handle port already taken
- [ ]  Share config path
- [ ]  Documentation of commands
