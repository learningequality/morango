# -*- coding: utf-8 -*-
# Generated by Django 1.11.29 on 2020-07-15 21:40
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("morango", "0016_store_deserialization_error"),
    ]

    operations = [
        migrations.AddField(
            model_name="syncsession",
            name="allow_resume",
            field=models.BooleanField(default=False),
        ),
    ]
