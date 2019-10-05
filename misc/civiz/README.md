# civiz

civiz visualizes CI results at http://mtrlz.dev/civiz.

## Developing locally

You'll need a recent version of Python 3 installed.

Then:

```shell
cd misc/civiz
source venv/bin/activate
python setup.py develop
```

You can exit the virtual environment ("virtualenv" or "venv", in Python
parlance) with the `deactivate` command.

To run the app, you'll need a PostgreSQL server running at localhost and
accessible to the current user without a username or password, and a
database named `buildkite` installed on the server.

To build the necessary database tables:

```shell
alembic upgrade head
```

To run the webserver:

```shell
FLASK_APP=civiz.py flask run
```

## Deploying

A successful merge to master will deploy the new code to mtrlz.dev/civiz.
