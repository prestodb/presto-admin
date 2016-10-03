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

from nose.plugins.attrib import attr

import prestoadmin

from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase, docker_only
from tests.product.cluster_types import STANDALONE_PRESTO_CLUSTER
from tests.product.standalone.presto_installer import StandalonePrestoInstaller


class TestServerUpgrade(BaseProductTestCase):

    def setUp(self):
        super(TestServerUpgrade, self).setUp()
        self.setup_cluster(NoHadoopBareImageProvider, STANDALONE_PRESTO_CLUSTER)
        self.dummy_installer = StandalonePrestoInstaller(
            self, (os.path.join(prestoadmin.main_dir, 'tests', 'product',
                                'resources'), 'dummy-rpm.rpm'))
        self.real_installer = StandalonePrestoInstaller(self)

    def start_and_assert_started(self):
        cmd_output = self.run_prestoadmin('server start')
        process_per_host = self.get_process_per_host(cmd_output.splitlines())
        self.assert_started(process_per_host)

    #
    # The dummy RPM is not guaranteed to have any functionality beyond not
    # including any real payload and adding the random README file. It's a
    # hacky one-off that satisfies the requirement of having *something* to
    # upgrade to without downloading another copy of the real RPM. This is NOT
    # the place to test functionality that the presto-server-rpm normally
    # provides, because the dummy rpm probably doesn't provide it, or worse,
    # provides an old and/or broken version of it.
    #
    def assert_upgraded_to_dummy_rpm(self, hosts):
        for container in hosts:
            # Still should have the same configs
            self.dummy_installer.assert_installed(self, container)
            self.assert_has_default_config(container)
            self.assert_has_default_connector(container)

            # However, dummy_rpm.rpm removes /usr/lib/presto/lib and
            # /usr/lib/presto/lib/plugin
            self.assert_path_removed(container, '/usr/lib/presto/lib')
            self.assert_path_removed(container, '/usr/lib/presto/lib/plugin')

            # And adds /usr/lib/presto/README.txt
            self.assert_path_exists(container, '/usr/lib/presto/README.txt')

            # And modifies the text of the readme in
            # /usr/shared/doc/presto/README.txt
            self.assert_file_content_regex(
                container,
                '/usr/shared/doc/presto/README.txt',
                r'.*New line of text here.$'
            )

    @attr('smoketest')
    def test_upgrade(self):
        self.start_and_assert_started()

        self.run_prestoadmin('configuration deploy')
        for container in self.cluster.all_hosts():
            self.real_installer.assert_installed(self, container)
            self.assert_has_default_config(container)
            self.assert_has_default_connector(container)

        path_on_cluster = self.copy_upgrade_rpm_to_cluster()
        self.upgrade_and_assert_success(path_on_cluster)

    def upgrade_and_assert_success(self, path_on_cluster, extra_arguments=''):
        self.run_prestoadmin('server upgrade ' + path_on_cluster + extra_arguments)
        self.assert_upgraded_to_dummy_rpm(self.cluster.all_hosts())

    @docker_only
    def test_rolling_upgrade(self):
        # Test that if a node is down, and then you upgrade again, it works
        self.run_prestoadmin('configuration deploy')

        self.cluster.stop_host(self.cluster.slaves[0])
        path_on_cluster = self.copy_upgrade_rpm_to_cluster()
        self.run_prestoadmin('server upgrade ' + path_on_cluster,
                             raise_error=False)
        running_hosts = self.cluster.all_hosts()[:]
        running_hosts.remove(self.cluster.slaves[0])
        self.assert_upgraded_to_dummy_rpm(running_hosts)

        self.cluster.start_host(self.cluster.slaves[0])
        self.retry(lambda: self.upgrade_and_assert_success(path_on_cluster))

    def copy_upgrade_rpm_to_cluster(self):
        rpm_name = self.dummy_installer.copy_presto_rpm_to_master()
        return os.path.join(self.cluster.rpm_cache_dir, rpm_name)

    def test_upgrade_fails_given_directory(self):
        dir_on_cluster = '/opt/prestoadmin'
        self.assertRaisesRegexp(
            OSError,
            'RPM file not found at %s.' % dir_on_cluster,
            self.run_prestoadmin,
            'server upgrade ' + dir_on_cluster
        )

    def test_upgrade_works_with_symlink(self):
        self.run_prestoadmin('configuration deploy')
        for container in self.cluster.all_hosts():
            self.real_installer.assert_installed(self, container)
            self.assert_has_default_config(container)
            self.assert_has_default_connector(container)

        path_on_cluster = self.copy_upgrade_rpm_to_cluster()
        symlink = '/opt/prestoadmin/link.rpm'
        self.cluster.exec_cmd_on_host(self.cluster.master, 'ln -s %s %s'
                                      % (path_on_cluster, symlink))
        self.upgrade_and_assert_success(symlink)

    def test_configuration_preserved_on_upgrade(self):
        self.run_prestoadmin('configuration deploy')
        for container in self.cluster.all_hosts():
            self.real_installer.assert_installed(self, container)
            self.assert_has_default_config(container)
            self.assert_has_default_connector(container)

        self.add_dummy_properties_to_host(self.cluster.slaves[1])
        path_on_cluster = self.copy_upgrade_rpm_to_cluster()
        symlink = '/opt/prestoadmin/link.rpm'
        self.cluster.exec_cmd_on_host(self.cluster.master, 'ln -s %s %s'
                                      % (path_on_cluster, symlink))

        self.run_prestoadmin('server upgrade ' + path_on_cluster)
        self.assert_dummy_properties(self.cluster.slaves[1])

    def test_upgrade_non_root_user(self):
        self.upload_topology(
            {"coordinator": "master",
             "workers": ["slave1", "slave2", "slave3"],
             "username": "app-admin"}
        )
        self.run_prestoadmin('configuration deploy -p password')
        for container in self.cluster.all_hosts():
            self.real_installer.assert_installed(self, container)
            self.assert_has_default_config(container)
            self.assert_has_default_connector(container)

        path_on_cluster = self.copy_upgrade_rpm_to_cluster()
        self.upgrade_and_assert_success(path_on_cluster, extra_arguments=' -p password')

    def add_dummy_properties_to_host(self, host):
        self.cluster.write_content_to_host(
            'com.facebook.presto=INFO',
            '/etc/presto/log.properties',
            host
        )
        self.cluster.write_content_to_host(
            'dummy config file',
            '/etc/presto/jvm.config',
            host
        )

    def assert_dummy_properties(self, host):
        # assert log properties file is there
        self.assert_file_content(
            host,
            '/etc/presto/log.properties',
            'com.facebook.presto=INFO'
        )

        # assert dummy jvm config is there too
        self.assert_file_content(
            host,
            '/etc/presto/jvm.config',
            'dummy config file'
        )
