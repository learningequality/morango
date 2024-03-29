# -*- coding: utf-8 -*-
# Generated by Django 1.11.13 on 2018-06-12 18:38
from django.db import migrations
from django.db import models

import morango.models.fields.crypto


class Migration(migrations.Migration):

    dependencies = [("morango", "0010_auto_20171206_1615")]

    operations = [
        migrations.CreateModel(
            name="SharedKey",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("public_key", morango.models.fields.crypto.PublicKeyField()),
                ("private_key", morango.models.fields.crypto.PrivateKeyField()),
                ("current", models.BooleanField(default=True)),
            ],
        )
    ]
