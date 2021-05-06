# Morango

[![build](https://github.com/learningequality/morango/actions/workflows/tox.yml/badge.svg?branch=master)](https://github.com/learningequality/morango/actions)
[![image](http://codecov.io/github/learningequality/morango/coverage.svg?branch=master)](http://codecov.io/github/learningequality/morango?branch=master)
[![image](https://readthedocs.org/projects/morango/badge/?version=latest)](http://morango.readthedocs.org/en/latest/)

Morango is a pure-Python database replication engine for Django that supports peer-to-peer syncing of data. It is structured as a Django app that can be included in projects to make specific application models syncable.

Developed in support of the [Kolibri](https://github.com/learningequality/kolibri) product ecosystem, Morango includes some important features including:

-   A certificate-based authentication system to protect privacy and integrity of data
-   A change-tracking system to support calculation of differences between databases across low-bandwidth connections
-   A set of constructs to support data partitioning

## Developer documentation

See [morango.readthedocs.io](https://morango.readthedocs.io)

To build and edit the docs, run:

```bash
# install requirements
pip install -r requirements/docs.txt
pip install -e .

# build docs
make docs

# auto-build and refresh docs on edit
make docs-autobuild
```
