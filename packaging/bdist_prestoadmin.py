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
from distutils import log as logger
from distutils.dir_util import remove_tree

import pip

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

    NATIVE_WHEELS = ['pycrypto-2.6.1-{0}-none-linux_x86_64.whl', 'twofish-0.3.0-{0}-none-linux_x86_64.whl']

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
        with open(os.path.join(package_dir, 'install-prestoadmin.template'), 'r') as template:
            with open(os.path.join(build_dir, 'install-prestoadmin.sh'), 'w') as install_script_file:
                install_script = self._fill_in_template(template.readlines(), wheel_name)
                install_script_file.write(install_script)
                os.chmod(os.path.join(build_dir, 'install-prestoadmin.sh'), 0755)

    def _fill_in_template(self, template_lines, wheel_name):
        if self.online_install:
            extra_install_args = ''
        else:
            extra_install_args = '--no-index --find-links third-party'

        filled_in = [self._replace_template_values(line, wheel_name, extra_install_args) for line in template_lines]
        return ''.join(filled_in)

    def _replace_template_values(self, line, wheel_name, extra_install_args):
        line = re.sub(r'%ONLINE_OR_OFFLINE_INSTALL%', extra_install_args, line)
        line = re.sub(r'%WHEEL_NAME%', wheel_name, line)
        line = re.sub(r'%VIRTUALENV_VERSION%', self.virtualenv_version, line)
        return line

    def package_dependencies(self, build_dir):
        thirdparty_dir = os.path.join(build_dir, 'third-party')

        requirements = self.distribution.install_requires
        for requirement in requirements:
            pip.main(['wheel',
                      '--wheel-dir={0}'.format(thirdparty_dir),
                      '--no-cache',
                      requirement])

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
        self.make_archive(archive_file, 'gztar',
                          root_dir=os.path.dirname(build_dir),
                          base_dir=os.path.basename(build_dir))
        logger.info('created %s.tar.gz', archive_file)

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
