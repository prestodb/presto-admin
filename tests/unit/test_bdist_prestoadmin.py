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
import re

from distutils.dir_util import remove_tree
from distutils.dir_util import mkpath
from mock import patch

from mock import call

from tests.base_test_case import BaseTestCase
from packaging.bdist_prestoadmin import bdist_prestoadmin
from distutils.dist import Distribution


# Hello future maintainer! Several tests in here include a version number in a
# path. It is by pure coincidence that these happen to match the current
# version number, if in fact they still do. We set the version number for the
# tests in self.attrs, and it can be anything as long as the other version
# numbers in the file match.
class TestBDistPrestoAdmin(BaseTestCase):
    def setUp(self):
        super(TestBDistPrestoAdmin, self).setUp()
        self.attrs = {
            'name': 'prestoadmin',
            'cmdclass': {'bdist_prestoadmin': bdist_prestoadmin},
            'version': '1.2',
            'packages': ['prestoadmin'],
            'package_dir': {'prestoadmin': 'prestoadmin'},
            'install_requires': ['fabric']
        }

        # instantiation of the object calls
        # initialize_options which is what we are testing
        dist = Distribution(attrs=self.attrs)
        self.bdist = dist.get_command_obj('bdist_prestoadmin')
        self.bdist.finalize_options()

    def test_initialize(self):
        # we don't use the dist from setUp because
        # we want to test before finalize is called
        dist = Distribution(attrs=self.attrs)
        bdist = dist.get_command_obj('bdist_prestoadmin')

        self.assertEquals(bdist.bdist_dir, None)
        self.assertEquals(bdist.dist_dir, None)
        self.assertEquals(bdist.virtualenv_version, None)
        self.assertEquals(bdist.keep_temp, False)
        self.assertEquals(bdist.online_install, False)

    def test_finalize(self):
        self.assertRegexpMatches(
            self.bdist.bdist_dir,
            'build/bdist.*/prestoadmin')
        self.assertEquals(self.bdist.dist_dir, 'dist')
        self.assertEquals(self.bdist.default_virtualenv_version, '12.0.7')
        self.assertEquals(self.bdist.keep_temp, False)

    def test_finalize_argvs(self):
        self.attrs['script_args'] = ['bdist_prestoadmin',
                                     '--bdist-dir=junk',
                                     '--dist-dir=tmp',
                                     '--virtualenv-version=12.0.1',
                                     '-k'
                                     ]

        # we don't use the dist from setUp because
        # we want to test with additional arguments
        dist = Distribution(attrs=self.attrs)
        dist.parse_command_line()
        bdist = dist.get_command_obj('bdist_prestoadmin')
        bdist.finalize_options()

        self.assertEquals(bdist.bdist_dir, 'junk')
        self.assertEquals(bdist.dist_dir, 'tmp')
        self.assertEquals(bdist.virtualenv_version, '12.0.1')
        self.assertEquals(bdist.keep_temp, True)

    @patch('distutils.core.Command.run_command')
    def test_build_wheel(self, run_command_mock):
        self.assertEquals('prestoadmin-1.2-py2-none-any',
                          self.bdist.build_wheel('build'))

    @patch('packaging.bdist_prestoadmin.pip.main')
    def test_package_dependencies_for_offline_installer(self, pip_mock):
        build_path = os.path.join('build', 'prestoadmin')
        self.bdist.package_dependencies(build_path)

        calls = [call(['wheel',
                       '--wheel-dir=build/prestoadmin/third-party',
                       '--no-cache',
                       'fabric']),
                 call(['install',
                       '-d',
                       'build/prestoadmin/third-party',
                       '--no-cache',
                       '--no-use-wheel',
                       'virtualenv==12.0.7'])]
        pip_mock.assert_has_calls(calls, any_order=False)

    @patch('packaging.bdist_prestoadmin.bdist_prestoadmin.'
           'generate_install_script')
    @patch('packaging.bdist_prestoadmin.bdist_prestoadmin.build_wheel')
    @patch('packaging.bdist_prestoadmin.bdist_prestoadmin.'
           'package_dependencies')
    def test_package_dependencies_for_online_installer(
            self, package_dependencies_mock, build_wheel_mock,
            generate_install_script_mock):
        self.bdist.online_install = True

        self.bdist.run()

        assert not package_dependencies_mock.called, 'method should not have been called'

    def test_generate_online_install_script(self):
        test_input = ['virtualenv-%VIRTUALENV_VERSION%.tar.gz\n',
                      'pip install %WHEEL_NAME%.whl %ONLINE_OR_OFFLINE_INSTALL%']
        self.bdist.online_install = True
        output = self.bdist._fill_in_template(test_input, 'my_wheel')
        self.assertEqual(output, 'virtualenv-12.0.7.tar.gz\npip install my_wheel.whl ')

    def test_generate_offline_install_script(self):
        test_input = ['virtualenv-%VIRTUALENV_VERSION%.tar.gz\n',
                      'pip install %WHEEL_NAME%.whl %ONLINE_OR_OFFLINE_INSTALL%']
        self.bdist.online_install = False
        output = self.bdist._fill_in_template(test_input, 'my_wheel')
        self.assertEqual(output,
                         'virtualenv-12.0.7.tar.gz\npip install my_wheel.whl --no-index --find-links third-party')

    def test_archive_dist_offline(self):
        build_path = os.path.join('build', 'prestoadmin')
        try:
            mkpath(build_path)
            self.bdist.archive_dist(build_path, 'dist')

            archive = os.path.join('dist', 'prestoadmin-1.2-offline.tar.gz')
            self.assertTrue(os.path.exists(archive))
        finally:
            remove_tree(os.path.dirname(build_path))
            remove_tree('dist')

    def test_archive_dist_online(self):
        build_path = os.path.join('build', 'prestoadmin')
        try:
            mkpath(build_path)
            self.bdist.online_install = True
            self.bdist.archive_dist(build_path, 'dist')

            archive = os.path.join('dist', 'prestoadmin-1.2-online.tar.gz')
            self.assertTrue(os.path.exists(archive))
        finally:
            remove_tree(os.path.dirname(build_path))
            remove_tree('dist')

    @patch('distutils.core.Command.mkpath')
    @patch('packaging.bdist_prestoadmin.remove_tree')
    @patch('packaging.bdist_prestoadmin.bdist_prestoadmin.build_wheel',
           return_value='wheel_name')
    @patch('packaging.bdist_prestoadmin.bdist_prestoadmin.' +
           'generate_install_script')
    @patch('packaging.bdist_prestoadmin.bdist_prestoadmin.' +
           'package_dependencies')
    @patch('packaging.bdist_prestoadmin.bdist_prestoadmin.archive_dist')
    def test_run(self,
                 archive_dist_mock,
                 package_dependencies_mock,
                 install_script_mock,
                 build_wheel_mock,
                 remove_tree_mock,
                 mkpath_mock):
        self.bdist.run()

        def matching_regex(expected_regex):
            class RegexMatcher:
                def __eq__(self, other):
                    return re.match(expected_regex, other)
            return RegexMatcher()

        build_path_re = matching_regex(
            'build/bdist.*/prestoadmin')
        build_wheel_mock.assert_called_once_with(build_path_re)
        install_script_mock.assert_called_once_with('wheel_name',
                                                    build_path_re)
        package_dependencies_mock.assert_called_once_with(
            build_path_re)
        archive_dist_mock.assert_called_once_with(build_path_re, 'dist')

    def test_description(self):
        self.assertEquals('create a distribution for prestoadmin',
                          self.bdist.description)

    def test_user_options(self):
        expected = [('bdist-dir=', 'b',
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

        self.assertEquals(expected, self.bdist.user_options)
