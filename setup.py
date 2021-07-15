#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import io
from setuptools import find_packages, setup

import morango

readme = io.open("README.md", mode="r", encoding="utf-8").read()

setup(
    name='morango',
    version=morango.__version__,
    description="Pure Python sqlite-based Django DB replication engine.",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Learning Equality",
    author_email='dev@learningequality.org',
    url='https://github.com/learningequality/morango',
    packages=find_packages(exclude=['tests', "tests.*"]),
    package_dir={'morango':
                 'morango'},
    include_package_data=True,
    install_requires=[
        "django<1.12",
        "django-mptt<0.10.0",
        "rsa>=3.4.2,<3.5",
        "djangorestframework==3.9.1",
        "django-ipware>=1.1.6,<1.2",
        "future==0.16.0",
        "requests",
        "ifcfg",
    ],
    license="MIT",
    zip_safe=False,
    keywords=['database', 'syncing', 'morango'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
