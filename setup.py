#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import os

from pip.req import parse_requirements
from setuptools import find_packages, setup

import morango


req_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "requirements.txt")
reqs = parse_requirements(req_file, session=False)
install_requires = [str(ir.req) for ir in reqs]

test_req_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "requirements/test.txt")
test_reqs = parse_requirements(test_req_file, session=False)
tests_require = [str(ir.req) for ir in test_reqs]

setup(
    name='morango',
    version=morango.__version__,
    description="Pure Python sqlite-based Django DB replication engine.",
    long_description=readme + '\n\n',
    author="Learning Equality",
    author_email='dev@learningequality.org',
    url='https://github.com/learningequality/morango',
    packages=find_packages(),
    package_dir={'morango':
                 'morango'},
    include_package_data=True,
    install_requires=install_requires,
    license="MIT license",
    zip_safe=False,
    keywords=['database', 'syncing', 'morango'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=tests_require,
)
