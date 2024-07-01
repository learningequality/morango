#!/usr/bin/env python
# -*- coding: utf-8 -*-
import io

from setuptools import find_packages
from setuptools import setup

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
        "django>=3,<4",
        "django-mptt>0.10.0",
        "rsa>=3.4.2,<3.5",
        "djangorestframework>3.10",
        "django-ipware==4.0.2",
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
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    python_requires=">=3.6,  <3.13",
)
