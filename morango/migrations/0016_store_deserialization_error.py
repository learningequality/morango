# -*- coding: utf-8 -*-
# Generated by Django 1.11.28 on 2020-06-10 23:48
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):

    dependencies = [
        ('morango', '0015_auto_20200508_2104'),
    ]

    operations = [
        migrations.AddField(
            model_name='store',
            name='deserialization_error',
            field=models.TextField(blank=True),
        ),
    ]
