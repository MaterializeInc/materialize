import re
import ast
import platform
from setuptools import setup, find_packages

_version_re = re.compile(r"__version__\s+=\s+(.*)")

with open("pgcli/__init__.py", "rb") as f:
    version = str(
        ast.literal_eval(_version_re.search(f.read().decode("utf-8")).group(1))
    )

description = "CLI for the Materialize streaming view engine. With auto-completion and syntax highlighting."

install_requirements = [
    "pgspecial>=1.11.5",
    "click >= 4.1",
    "Pygments >= 2.0",  # Pygments has to be Capitalcased. WTF?
    "prompt_toolkit>=2.0.6,<2.1.0",
    "psycopg2 >= 2.7.4",
    "sqlparse >=0.3.0,<0.4",
    "configobj >= 5.0.6",
    "humanize >= 0.5.1",
    "cli_helpers[styles] >= 1.2.0",
]


# setproctitle is used to mask the password when running `ps` in command line.
# But this is not necessary in Windows since the password is never shown in the
# task manager. Also setproctitle is a hard dependency to install in Windows,
# so we'll only install it if we're not in Windows.
if platform.system() != "Windows" and not platform.system().startswith("CYGWIN"):
    install_requirements.append("setproctitle >= 1.1.9")

setup(
    name="mzcli",
    author="Pgcli Core Team + The Materialize developers",
    author_email="pgcli-dev@googlegroups.com",
    version=version,
    license="BSD",
    url="http://mtrlz.dev",
    packages=find_packages(),
    package_data={"pgcli": ["mzclirc", "packages/pgliterals/pgliterals.json"]},
    description=description,
    long_description=open("README.rst").read(),
    install_requires=install_requirements,
    extras_require={"keyring": ["keyring >= 12.2.0"]},
    entry_points="""
        [console_scripts]
        mzcli=pgcli.main:cli
    """,
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: SQL",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
