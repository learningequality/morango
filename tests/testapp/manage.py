#!/usr/bin/env python
import os
import sys

import morango # noqa F401
# Import morango to ensure that we do the monkey patching needed
# for Django 1.11 to work with Python 3.10+

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "testapp.settings")

    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)
