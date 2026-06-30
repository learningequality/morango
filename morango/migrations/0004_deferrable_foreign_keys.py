"""
Make existing foreign key constraints ``DEFERRABLE INITIALLY DEFERRED``.

Databases created before Morango 0.8 (with Django 1.11) have immediate SQLite foreign key
constraints, which can break cascade deletions (e.g. during sync) now that foreign keys are enforced
at the database level. See https://github.com/learningequality/kolibri/issues/14884.
"""
from django.db import migrations

from morango.deferrable_foreign_keys import MakeForeignKeysDeferrable


class Migration(migrations.Migration):

    dependencies = [
        ("morango", "0003_store_deserialization_errors"),
    ]

    operations = [
        migrations.RunPython(
            MakeForeignKeysDeferrable(include_app_labels=["morango"]),
            migrations.RunPython.noop,
        ),
    ]
