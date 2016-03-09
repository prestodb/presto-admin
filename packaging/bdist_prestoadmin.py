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

import os
import re
import pip
import sys
import urllib

from distutils.dir_util import remove_tree
from distutils import log as logger

try:
    from setuptools import Command
except ImportError:
    from distutils.core import Command

from packaging import package_dir


class bdist_prestoadmin(Command):

    description = 'create a distribution for prestoadmin'

    user_options = [('bdist-dir=', 'b',
                     'temporary directory for creating the distribution'),
                    ('dist-dir=', 'd',
                     'directory to put final built distributions in'),
                    ('virtualenv-version=', None,
                     'version of virtualenv to download'),
                    ('keep-temp', 'k',
                     'keep the pseudo-installation tree around after ' +
                     'creating the distribution archive'),
                    ('online-install', None, 'boolean flag indicating if ' +
                     'the installation should pull dependencies from the ' +
                     'Internet or use the ones supplied in the third party ' +
                     'directory')
                    ]

    default_virtualenv_version = '12.0.7'

    def build_wheel(self, build_dir):
        cmd = self.reinitialize_command('bdist_wheel')
        cmd.dist_dir = build_dir
        self.run_command('bdist_wheel')

        # Ensure that you get the finalized archive name
        cmd.finalize_options()
        wheel_name = cmd.get_archive_basename()
        logger.info('creating %s in %s', wheel_name + '.whl', build_dir)

        return wheel_name

    def generate_install_script(self, wheel_name, build_dir):
        template = open(os.path.join(package_dir,
                                     'install-prestoadmin.template'), 'r')
        install_script = open(os.path.join(build_dir,
                              'install-prestoadmin.sh'), 'w')
        if self.online_install:
            extra_install_args = ''
        else:
            extra_install_args = '--no-index --find-links third-party'

        for line in template.readlines():
            line = re.sub(r'%ONLINE_OR_OFFLINE_INSTALL%',
                          extra_install_args, line)
            line = re.sub(r'%WHEEL_NAME%', wheel_name, line)
            line = re.sub(r'%VIRTUALENV_VERSION%', self.virtualenv_version,
                          line)
            install_script.write(line)
        install_script.close()
        template.close()
        os.chmod(os.path.join(build_dir, 'install-prestoadmin.sh'), 0o755)

    def package_dependencies(self, build_dir):
        thirdparty_dir = os.path.join(build_dir, 'third-party')

        requirements = self.distribution.install_requires
        for requirement in requirements:
            pip.main(['wheel',
                      '--wheel-dir={0}'.format(thirdparty_dir),
                      '--no-cache',
                      requirement,
                      '--extra-index-url',
                      'http://bdch-ftp.td.teradata.com:8082',
                      '--trusted-host',
                      'bdch-ftp.td.teradata.com'])

        # Welcome to HackLand! For our offline installer we need to
        # include the pycrypto wheel compiled both against the Python
        # 2.6 and 2.7 interpreters. We found no way to do that at build
        # time (either compile against both interpreters simultaneously
        # or somehow compile first against 2.6 and then against 2.7
        # serially). To solve this we pre-compiled both and uploaded to
        # the internal PyPI. During the build we download wheels for both
        # interpreters compiled on Centos 6.6.
        pycrypto_whl = 'pycrypto-2.6.1-{0}-none-linux_x86_64.whl'
        pypi_pycrypto_url = 'http://bdch-ftp.td.teradata.com:8082/' + \
                            'packages/' + pycrypto_whl
        if sys.version.startswith('2.6'):
            alternate_interpreter_version = 'cp27'  # fetch 2.7 from PyPI
        else:
            alternate_interpreter_version = 'cp26'

        urllib.urlretrieve(
            pypi_pycrypto_url.format(alternate_interpreter_version),
            os.path.join(
                thirdparty_dir,
                pycrypto_whl.format(alternate_interpreter_version))
            )
        # Thank you for visiting HackLand!

        pip.main(['install',
                  '-d',
                  thirdparty_dir,
                  '--no-cache',
                  '--no-use-wheel',
                  'virtualenv=={0}'.format(self.virtualenv_version)])

    def archive_dist(self, build_dir, dist_dir):
        archive_basename = self.distribution.get_fullname()
        if self.online_install:
            archive_basename += '-online'
        else:
            archive_basename += '-offline'
        archive_file = os.path.join(dist_dir, archive_basename)
        self.mkpath(os.path.dirname(archive_file))
        self.make_archive(archive_file, 'bztar',
                          root_dir=os.path.dirname(build_dir),
                          base_dir=os.path.basename(build_dir))
        logger.info('created %s.tar.bz2', archive_file)

    def run(self):
        build_dir = self.bdist_dir
        self.mkpath(build_dir)

        wheel_name = self.build_wheel(build_dir)
        self.generate_install_script(wheel_name, build_dir)
        if not self.online_install:
            self.package_dependencies(build_dir)

        self.archive_dist(build_dir, self.dist_dir)

        if not self.keep_temp:
            remove_tree(build_dir)

    def initialize_options(self):
        self.bdist_dir = None
        self.dist_dir = None
        self.virtualenv_url_base = None
        self.virtualenv_version = None
        self.keep_temp = False
        self.online_install = False

    def finalize_options(self):
        if self.bdist_dir is None:
            bdist_base = self.get_finalized_command('bdist').bdist_base
            self.bdist_dir = os.path.join(bdist_base,
                                          self.distribution.get_name())

        if self.dist_dir is None:
            self.dist_dir = 'dist'

        if self.virtualenv_version is None:
            self.virtualenv_version = self.default_virtualenv_version
