# Morango

[![build](https://github.com/learningequality/morango/actions/workflows/tox.yml/badge.svg?branch=master)](https://github.com/learningequality/morango/actions)
[![image](http://codecov.io/github/learningequality/morango/coverage.svg?branch=master)](http://codecov.io/github/learningequality/morango?branch=master)
[![image](https://readthedocs.org/projects/morango/badge/?version=latest)](http://morango.readthedocs.org/en/latest/)

Morango is a pure-Python database replication engine for Django that supports peer-to-peer syncing of data. It is structured as a Django app that can be included in projects to make specific application models syncable.

Developed in support of the [Kolibri](https://github.com/learningequality/kolibri) product ecosystem, Morango includes some important features including:

-   A certificate-based authentication system to protect privacy and integrity of data
-   A change-tracking system to support calculation of differences between databases across low-bandwidth connections
-   A set of constructs to support data partitioning
-   Support for SQLite and PostgreSQL

## Developer documentation

See [morango.readthedocs.io](https://morango.readthedocs.io) for documentation on how Morango works.

### Getting started

To start contributing to Morango, first make sure you [have `uv` installed](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, which will create it in the `.venv/` directory, with the python version defined in `.python-version`:
```bash
uv venv
```

Then install dependencies:
```bash
uv sync --all-extras
```

If you get errors during installation, you may need to install system packages such as `openssl` and `libssl-dev`.

Finally, set up pre-commit hooks:
```bash
prek install  # with -f to reinstall
```

### Building
Building the project is as easy as:
```bash
uv build
```
Afterwards, you'll find a source archive and wheel file in `dist/`.

### Docs

To build and edit the docs, run:

```bash
# install requirements (if necessary)
uv sync --extra docs

# build docs
make docs

# auto-build and refresh docs on edit
make docs-autobuild
```
