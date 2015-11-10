#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

# This is necessary for nose to handle multiprocessing correctly
from multiprocessing import util

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

from packaging.bdist_prestoadmin import bdist_prestoadmin

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read().replace('.. :changelog:', '')

# Requirements for both development and testing are duplicated here
# and in the requirements.txt. Unfortunately this is required by
# tox which relies on the existence of both.
requirements = [
    'paramiko==1.15.3',
    'fabric==1.10.1',
    'requests==2.7.0'
]

test_requirements = [
    'tox==1.9.2',
    'nose==1.3.7',
    'mock==1.0.1',
    'wheel==0.23.0',
    'docker-py==1.2.2',
    'certifi==2015.4.28',
    'fudge==1.1.0',
    'PyYAML==3.11'
]

# =====================================================
# Welcome to HackLand! We monkey patch the _get_rc_file
# method of PyPIRCCommand so that we can read a .pypirc
# that is located in the current directory. This enables
# us to check it in with the code and not require
# developers to create files in their home directory.
from distutils.config import PyPIRCCommand


def get_custom_rc_file(self):
    home_pypi = os.path.join(os.path.expanduser('~'),
                             '.pypirc')
    local_pypi = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        '.pypirc')
    return local_pypi if os.path.exists(local_pypi) \
        else home_pypi

PyPIRCCommand._get_rc_file = get_custom_rc_file
# Thank you for visiting HackLand!
# =====================================================

setup(
    name='prestoadmin',
    version='1.2',  # Make sure to update version in prestoadmin/__init__.py
    description="Presto-admin installs, configures, and manages Presto installations.",
    long_description=readme + '\n\n' + history,
    author="Teradata Coporation",
    author_email='christina.wallin@teradata.com',
    url='https://github.com/teradatalabs/presto-admin',
    packages=find_packages(exclude=['*tests*']),
    package_dir={'prestoadmin':
                 'prestoadmin'},
    package_data={'prestoadmin': ['presto-admin-logging.ini']},
    include_package_data=True,
    install_requires=requirements,
    license="APLv2",
    zip_safe=False,
    keywords='prestoadmin',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: APLv2 License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7'
    ],
    test_suite='tests',
    tests_require=test_requirements,
    cmdclass={'bdist_prestoadmin': bdist_prestoadmin},
    entry_points={'console_scripts': ['presto-admin = prestoadmin.main:main']}
)
