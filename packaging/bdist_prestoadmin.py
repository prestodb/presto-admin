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
                     "temporary directory for creating the distribution"),
                    ('dist-dir=', 'd',
                     "directory to put final built distributions in"),
                    ('virtualenv-version=', None,
                     "version of virtualenv to download"),
                    ('keep-temp', 'k',
                     "keep the pseudo-installation tree around after " +
                     "creating the distribution archive")
                    ]

    default_virtualenv_version = '12.0.7'

    def build_wheel(self, build_dir):
        cmd = self.reinitialize_command('bdist_wheel')
        cmd.dist_dir = build_dir
        wheel_name = cmd.get_archive_basename()
        logger.info('creating %s in %s', wheel_name + '.whl', build_dir)
        self.run_command('bdist_wheel')

        return wheel_name

    def generate_install_script(self, wheel_name, build_dir):
        template = open(os.path.join(package_dir,
                                     'install-prestoadmin.template'), 'r')
        install_script = open(os.path.join(build_dir,
                              'install-prestoadmin.sh'), 'w')
        for line in template.readlines():
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
                      requirement])

        pip.main(['install',
                  '-d',
                  thirdparty_dir,
                  '--no-cache',
                  '--no-use-wheel',
                  'virtualenv=={0}'.format(self.virtualenv_version)])

    def archive_dist(self, build_dir, dist_dir):
        archive_basename = self.distribution.get_fullname()
        archive_file = os.path.join(dist_dir, archive_basename)
        self.mkpath(os.path.dirname(archive_file))
        self.make_archive(archive_file, 'bztar',
                          root_dir=os.path.dirname(build_dir))
        logger.info('created %s.tar.bz2', archive_file)

    def run(self):
        build_dir = self.bdist_dir
        self.mkpath(build_dir)

        wheel_name = self.build_wheel(build_dir)
        self.generate_install_script(wheel_name, build_dir)
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

    def finalize_options(self):
        if self.bdist_dir is None:
            bdist_base = self.get_finalized_command('bdist').bdist_base
            self.bdist_dir = os.path.join(bdist_base,
                                          self.distribution.get_name())

        if self.dist_dir is None:
            self.dist_dir = 'dist'

        if self.virtualenv_version is None:
            self.virtualenv_version = self.default_virtualenv_version
