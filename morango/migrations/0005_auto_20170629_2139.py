# -*- coding: utf-8 -*-
# Generated by Django 1.9.7 on 2017-06-29 21:39
from django.db import migrations

import morango.models.fields.crypto


class Migration(migrations.Migration):

    dependencies = [("morango", "0004_auto_20170520_2112")]

    operations = [
        migrations.RenameField(
            model_name="certificate", old_name="private_key", new_name="_private_key"
        ),
        migrations.AlterField(
            model_name="certificate",
            name="_private_key",
            field=morango.models.fields.crypto.PrivateKeyField(
                blank=True, db_column="private_key", null=True
            ),
        ),
    ]
