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
Product tests for presto-admin installation
"""
import certifi
import os

from nose.plugins.attrib import attr

from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase, docker_only
from tests.product.cluster_types import STANDALONE_BARE_CLUSTER
from tests.product.config_dir_utils import get_connectors_directory, get_coordinator_directory, get_workers_directory
from tests.product.prestoadmin_installer import PrestoadminInstaller
from tests.docker_cluster import DockerCluster
from tests.product.constants import BASE_IMAGES_TAG
from tests.product.constants import DEFAULT_DOCKER_MOUNT_POINT, \
    DEFAULT_LOCAL_MOUNT_POINT


class TestInstallation(BaseProductTestCase):

    def setUp(self):
        super(TestInstallation, self).setUp()
        self.pa_installer = PrestoadminInstaller(self)
        self.setup_cluster(NoHadoopBareImageProvider, STANDALONE_BARE_CLUSTER)
        dist_dir = self.pa_installer._build_dist_if_necessary(self.cluster)
        self.pa_installer._copy_dist_to_host(self.cluster, dist_dir, self.cluster.master)

    @attr('smoketest')
    @docker_only
    def test_install_non_root(self):
        install_dir = '/home/app-admin'
        script = """
            set -e
            cp {mount_dir}/prestoadmin-*.tar.bz2 {install_dir}
            chown app-admin {install_dir}/prestoadmin-*.tar.bz2
            cd {install_dir}
            sudo -u app-admin tar jxf prestoadmin-*.tar.bz2
            cd prestoadmin
            sudo -u app-admin ./install-prestoadmin.sh
        """.format(mount_dir=self.cluster.mount_dir, install_dir=install_dir)

        self.cluster.run_script_on_host(script, self.cluster.master)

        pa_config_dir = '/home/app-admin/.prestoadmin'
        connectors_dir = os.path.join(pa_config_dir, 'connectors')
        self.assert_path_exists(self.cluster.master, connectors_dir)

        coordinator_dir = os.path.join(pa_config_dir, 'coordinator')
        self.assert_path_exists(self.cluster.master, coordinator_dir)

        workers_dir = os.path.join(pa_config_dir, 'workers')
        self.assert_path_exists(self.cluster.master, workers_dir)

    @attr('smoketest', 'offline_installer')
    @docker_only
    def test_install_on_wrong_os_offline_installer(self):
        image = 'teradatalabs/ubuntu-trusty-python2.6'
        tag = BASE_IMAGES_TAG
        host = 'wrong-os-master'
        ubuntu_container = DockerCluster(host, [], DEFAULT_LOCAL_MOUNT_POINT, DEFAULT_DOCKER_MOUNT_POINT)

        if not DockerCluster._check_for_images(image, image, tag):
            raise RuntimeError("Docker images have not been created")

        try:
            ubuntu_container.start_containers(image + ':' + tag, cmd='tail -f /var/log/bootstrap.log')

            self.assertRaisesRegexp(
                OSError,
                r'ERROR\n'
                r'Paramiko could not be imported. This usually means that',
                self.pa_installer.install,
                cluster=ubuntu_container
            )
        finally:
            ubuntu_container.tear_down()

    @attr('smoketest')
    def test_cert_arg_to_installation_nonexistent_file(self):
        install_dir = '~'
        script = """
            set -e
            cp {mount_dir}/prestoadmin-*.tar.bz2 {install_dir}
            cd {install_dir}
            tar jxf prestoadmin-*.tar.bz2
            cd prestoadmin
             ./install-prestoadmin.sh dummy_cert.cert
        """.format(mount_dir=self.cluster.mount_dir,
                   install_dir=install_dir)
        output = self.cluster.run_script_on_host(script, self.cluster.master)
        self.assertRegexpMatches(output, r'Adding pypi.python.org as '
                                 'trusted\-host. Cannot find certificate '
                                 'file: dummy_cert.cert')

    @attr('smoketest')
    def test_cert_arg_to_installation_real_cert(self):
        self.cluster.copy_to_host(certifi.where(), self.cluster.master)
        install_dir = '~'
        cert_file = os.path.basename(certifi.where())
        script = """
            set -e
            cp {mount_dir}/prestoadmin-*.tar.bz2 {install_dir}
            cd {install_dir}
            tar jxf prestoadmin-*.tar.bz2
            cd prestoadmin
             ./install-prestoadmin.sh {mount_dir}/{cacert}
        """.format(mount_dir=self.cluster.mount_dir,
                   install_dir=install_dir,
                   cacert=cert_file)
        output = self.cluster.run_script_on_host(script, self.cluster.master)
        self.assertTrue('Adding pypi.python.org as trusted-host. Cannot find'
                        ' certificate file: %s' % cert_file not in output,
                        'Unable to find cert file; output: %s' % output)

    def test_additional_dirs_created(self):
        install_dir = '~'
        script = """
            set -e
            cp {mount_dir}/prestoadmin-*.tar.bz2 {install_dir}
            cd {install_dir}
            tar jxf prestoadmin-*.tar.bz2
            cd prestoadmin
             ./install-prestoadmin.sh
        """.format(mount_dir=self.cluster.mount_dir,
                   install_dir=install_dir)
        self.cluster.run_script_on_host(script, self.cluster.master)

        self.assert_path_exists(self.cluster.master, get_connectors_directory())
        self.assert_path_exists(self.cluster.master, get_coordinator_directory())
        self.assert_path_exists(self.cluster.master, get_workers_directory())
