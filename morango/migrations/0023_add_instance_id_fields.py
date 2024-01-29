# -*- coding: utf-8 -*-
# Generated by Django 1.11.29 on 2023-01-31 19:03
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):

    dependencies = [
        ('morango', '0022_rename_instance_fields'),
    ]

    operations = [
        migrations.AddField(
            model_name='syncsession',
            name='client_instance_id',
            field=models.UUIDField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='syncsession',
            name='server_instance_id',
            field=models.UUIDField(blank=True, null=True),
        ),
    ]
