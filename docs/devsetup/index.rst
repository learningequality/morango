Dev Setup
========

Before getting started, ensure you have the following dependencies installed:

- ``python`` (2.7 or 3.6 - 3.11)
- ``swig``
- ``openssl``
- ``docker-compose`` (for testing against postgres backends)

Optionally create a virtual environment with your Python setup for this project, then run the following commands::

    pip install -r requirements/dev.txt
    pip install -r requirements/test.txt
    # for testing with postgres: this might require a local install of a postgres package
    pip install -r requirements/postgres.txt


Testing
-------

Tests can be launched as follows::

    make test
    # launch against a postgres backend
    make test-with-postgres

The integration tests can be found in the `Kolibri repository <https://github.com/learningequality/kolibri/blob/develop/kolibri/core/auth/test/test_morango_integration.py>`_.

