# Materialize CLI

```
Command-line interface for Materialize.

USAGE:
    mz [OPTIONS] <SUBCOMMAND>

OPTIONS:
    -h, --help                 Print help information
    -p, --profile <PROFILE>    Identify using a particular configuration profile

SUBCOMMANDS:
    app-password    Show commands to interact with passwords
    docs            Open the docs
    help            Print this message or the help of the given subcommand(s)
    login           Open the web login
    region          Show commands to interact with regions
    shell           Open a SQL shell over a region
```

## Documentation

### Install

Clone the repository and install the CLI package:

```bash
cargo install --path ./src/mz
```

**[Required dependencies](#Dependencies)**

### Configuration

Interaction with regions requires a profile. There are two ways to configure one:

- Login command
- Configuration file

The quickest way to create a _default_ profile is by running the login command:

```bash
mz login
```

If you prefer to type your email and password in the console use the `--interactive` option:

```bash
mz login --interactive
```

After a successful login, the CLI will create and populate the configuration file with a _default_ profile.

### Configuration file

The configuration file stores all the available profiles. You can add your own as follows:

```TOML
["profiles.PROFILE_NAME"]
email = "your@email.com"
app_password = "YOUR_APP_PASSWORD"
```

#### Paths
* Linux: `.config/mz/profiles.toml`

* macOS (Monterrey): `.config/mz/profiles.toml`

* Windows: `%UserProfile%\.config\mz\profiles.toml`

Example:
```TOML
["profiles.production"]
email = "account@example.com"
app-password = "mzp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

["profiles.staging"]
email = "account@example.com"
app-password = "mzp_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
```

### App-Passwords

Create a new app-password to access Materialize's regions:

```bash
mz app-password create <NAME>
```

Make sure to store the password safely after its creation. App-passwords aren't recoverable.

List all the app-passwords names available:
```bash
mz app-password list
```

### Regions

Enable Materialize in a region:

```bash
mz region enable aws/us-east-1
```

List all the enabled region:

```bash
mz region list
```

Check any enabled region's status:

```bash
mz region status aws/us-east-1
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

Install [`rust` and `cargo`](https://doc.rust-lang.org/cargo/getting-started/installation.html):

```bash
# Linux & macOS
curl https://sh.rustup.rs -sSf | sh

# Windows
https://win.rustup.rs/
```

Install `psql` dependency for the shell command:

```bash
# Linux
apt-get install postgresql-client

# macOS
brew install libpq

echo 'export PATH="/usr/local/opt/libpq/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

echo 'export PATH="/usr/local/opt/libpq/bin:$PATH"' >> ~/.bash_profile
source ~/.bash_profile

# Windows (TBC)
```
