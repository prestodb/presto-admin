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

from tests.product.base_product_case import BaseProductTestCase, \
    DEFAULT_DOCKER_MOUNT_POINT, DEFAULT_LOCAL_MOUNT_POINT
from tests.docker_cluster import DockerCluster

install_py26_script = """\
echo "deb http://ppa.launchpad.net/fkrull/deadsnakes/ubuntu trusty main" \
    > /etc/apt/sources.list.d/fkrull-deadsnakes-trusty.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys \
    DB82666C
sudo apt-get update
sudo apt-get -y install python2.6
ln -s /usr/bin/python2.6 /usr/bin/python
"""


class TestInstallation(BaseProductTestCase):

    def setUp(self):
        super(TestInstallation, self).setUp()
        self.setup_docker_cluster()
        dist_dir = self.build_dist_if_necessary()
        self.copy_dist_to_host(dist_dir, self.docker_cluster.master)

    @attr('smoketest')
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
        """.format(mount_dir=self.docker_cluster.docker_mount_dir,
                   install_dir=install_dir)

        self.assertRaisesRegexp(OSError, 'mkdir: cannot create directory '
                                '`/var/log/prestoadmin\': Permission denied',
                                self.docker_cluster.run_script, script,
                                self.docker_cluster.master)

    @attr('smoketest')
    def test_install_from_different_dir(self):
        install_dir = '/opt'
        script = """
            set -e
            cp {mount_dir}/prestoadmin-*.tar.bz2 {install_dir}
            cd {install_dir}
            tar jxf prestoadmin-*.tar.bz2
             ./prestoadmin/install-prestoadmin.sh
        """.format(mount_dir=self.docker_cluster.docker_mount_dir,
                   install_dir=install_dir)

        self.assertRaisesRegexp(
            OSError,
            r'IOError: \[Errno 2\] No such file or directory: '
            r'\'/opt/prestoadmin-0.1.0-py2-none-any.whl\'',
            self.docker_cluster.run_script,
            script,
            self.docker_cluster.master
        )

    @attr('smoketest')
    def test_install_on_wrong_os_offline_installer(self):
        image = 'ubuntu'
        tag = '14.04'
        host = image + '-' + self.docker_cluster.master
        ubuntu_container = DockerCluster(host, [], DEFAULT_LOCAL_MOUNT_POINT,
                                         DEFAULT_DOCKER_MOUNT_POINT)
        try:
            ubuntu_container.fetch_image_if_not_present(image, tag)
            ubuntu_container.start_containers(
                image + ':' + tag, cmd='tail -f /var/log/bootstrap.log')

            self.docker_cluster.run_script(install_py26_script, host)
            ubuntu_container.exec_cmd_on_container(
                host, 'sudo apt-get -y install wget')

            self.assertRaisesRegexp(
                OSError,
                r'ERROR\n'
                r'Paramiko could not be imported. This usually means that',
                self.install_presto_admin,
                host
            )
        finally:
            ubuntu_container.tear_down_containers()

    @attr('smoketest')
    def test_cert_arg_to_installation_nonexistent_file(self):
        install_dir = '/opt'
        script = """
            set -e
            cp {mount_dir}/prestoadmin-*.tar.bz2 {install_dir}
            cd {install_dir}
            tar jxf prestoadmin-*.tar.bz2
            cd prestoadmin
             ./install-prestoadmin.sh dummy_cert.cert
        """.format(mount_dir=self.docker_cluster.docker_mount_dir,
                   install_dir=install_dir)
        output = self.docker_cluster.run_script(script,
                                                self.docker_cluster.master)
        self.assertRegexpMatches(output, r'Adding pypi.python.org as '
                                 'trusted\-host. Cannot find certificate '
                                 'file: dummy_cert.cert')

    @attr('smoketest')
    def test_cert_arg_to_installation_real_cert(self):
        self.docker_cluster.copy_to_host(certifi.where(),
                                         self.docker_cluster.master)
        install_dir = '/opt'
        cert_file = os.path.basename(certifi.where())
        script = """
            set -e
            cp {mount_dir}/prestoadmin-*.tar.bz2 {install_dir}
            cd {install_dir}
            tar jxf prestoadmin-*.tar.bz2
            cd prestoadmin
             ./install-prestoadmin.sh {mount_dir}/{cacert}
        """.format(mount_dir=self.docker_cluster.docker_mount_dir,
                   install_dir=install_dir,
                   cacert=cert_file)
        output = self.docker_cluster.run_script(script,
                                                self.docker_cluster.master)
        self.assertTrue('Adding pypi.python.org as trusted-host. Cannot find'
                        ' certificate file: %s' % cert_file not in output,
                        'Unable to find cert file; output: %s' % output)

    def is_image_present_locally(self, image_name, tag):
        image_name_and_tag = image_name + ':' + tag
        images = self.docker_client.images(image_name)
        if images:
            for image in images:
                if image['RepoTags'] is image_name_and_tag:
                    return True
        return False
