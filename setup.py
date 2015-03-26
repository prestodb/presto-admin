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
#

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read().replace('.. :changelog:', '')

requirements = [
    'fabric'
]

test_requirements = [
    'tox'
]

setup(
    name='prestoadmin',
    version='0.1.0',  # Make sure to update version in prestoadmin/__init__.py
    description="Presto-admin installs, configures, and manages Presto installations.",
    long_description=readme + '\n\n' + history,
    author="Teradata Coporation",
    author_email='christina.wallin@teradata.com',
    url='https://github.com/teradatalabs/presto-admin',
    packages=[
        'prestoadmin',
    ],
    package_dir={'prestoadmin':
                 'prestoadmin'},
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
    tests_require=test_requirements
)
