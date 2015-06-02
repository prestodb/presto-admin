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

"""
Product tests for generating an online and offline installer for presto-admin
"""
import subprocess
import os
import fnmatch
import re

from prestoadmin import main_dir
from tests.utils import run_make, BaseTestCase


class TestInstaller(BaseTestCase):

    def tearDown(self):
        run_make(['clean-build'])
        BaseTestCase.tearDown(self)

    def test_online_installer(self):
        run_make(['dist-online'])
        self._verify_third_party_dir(False)

    def test_offline_installer(self):
        run_make(['dist'])
        self._verify_third_party_dir(True)

    def _verify_third_party_dir(self, is_third_party_present):
        dist_dir = os.path.join(main_dir, 'dist')
        matches = fnmatch.filter(os.listdir(dist_dir), 'prestoadmin-*.tar.bz2')
        if len(matches) > 1:
            raise RuntimeError(
                'More than one archive found in the dist directory ' +
                ' '.join(matches)
            )
        cmd_to_run = ['tar', '-tf', os.path.join(dist_dir, matches[0])]
        popen_obj = subprocess.Popen(cmd_to_run,
                                     cwd=main_dir, stdout=subprocess.PIPE)
        retcode = popen_obj.returncode
        if retcode:
            raise RuntimeError('Non zero return code when executing ' +
                               ' '.join(cmd_to_run))
        stdout = popen_obj.communicate()[0]
        match = re.search('/third-party/', stdout)
        if is_third_party_present and match is None:
            raise RuntimeError('Expected to have an offline installer with '
                               'a third-party directory. Found no '
                               'third-party directory in the installer '
                               'archive.')
        elif not is_third_party_present and match:
            raise RuntimeError('Expected to have an online installer with no '
                               'third-party directory. Found a third-party '
                               'directory in the installer archive.')
