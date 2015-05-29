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

from tests.product.base_product_case import BaseProductTestCase, \
    DOCKER_MOUNT_POINT, LOCAL_MOUNT_POINT


class TestInstallation(BaseProductTestCase):

    def setUp(self):
        BaseProductTestCase.setUp(self)
        dist_dir = self.build_dist_if_necessary()
        self.copy_dist_to_master(dist_dir)

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
        """.format(mount_dir=DOCKER_MOUNT_POINT, install_dir=install_dir)

        self.assertRaisesRegexp(OSError, 'mkdir: cannot create directory '
                                '`/var/log/prestoadmin\': Permission denied',
                                self.run_script, script)

    def test_install_from_different_dir(self):
        install_dir = '/opt'
        script = """
            set -e
            cp {mount_dir}/prestoadmin-*.tar.bz2 {install_dir}
            cd {install_dir}
            tar jxf prestoadmin-*.tar.bz2
             ./prestoadmin/install-prestoadmin.sh
        """.format(mount_dir=DOCKER_MOUNT_POINT, install_dir=install_dir)

        self.assertRaisesRegexp(
            OSError,
            r'IOError: \[Errno 2\] No such file or directory: '
            r'\'/opt/prestoadmin-0.1.0-py2-none-any.whl\'',
            self.run_script,
            script
        )

    def test_install_on_wrong_os_offline_installer(self):
        self.tear_down_docker_cluster()
        self.create_host_mount_dirs()
        image = 'ubuntu'
        if not self.client.images(image):
            self._execute_and_wait(self.client.pull, image, '14.04')

        self._execute_and_wait(self.client.create_container,
                               image + ':14.04',
                               command='tail -f /var/log/bootstrap.log',
                               detach=True,
                               name=self.master,
                               hostname=self.master,
                               volumes=LOCAL_MOUNT_POINT % self.master)
        self.client.start(self.master,
                          binds={LOCAL_MOUNT_POINT % self.master:
                                 {"bind": DOCKER_MOUNT_POINT,
                                  "ro": False}})

        self.exec_create_start(self.master, 'sudo apt-get -y install'
                               ' python-minimal')
        self.exec_create_start(self.master, 'sudo apt-get -y install wget')
        self.install_presto_admin()

        self.assertRaisesRegexp(
            OSError,
            r'There was a problem importing our SSH library \(see traceback '
            r'above\).\nPlease make sure all dependencies are installed and '
            r'importable.',
            self.run_prestoadmin,
            '--help'
        )

    def test_cert_arg_to_installation_nonexistent_file(self):
        install_dir = '/opt'
        script = """
            set -e
            cp {mount_dir}/prestoadmin-*.tar.bz2 {install_dir}
            cd {install_dir}
            tar jxf prestoadmin-*.tar.bz2
            cd prestoadmin
             ./install-prestoadmin.sh dummy_cert.cert
        """.format(mount_dir=DOCKER_MOUNT_POINT, install_dir=install_dir)
        output = self.run_script(script)
        self.assertRegexpMatches(output, r'Adding pypi.python.org as '
                                 'trusted\-host. Cannot find certificate '
                                 'file: dummy_cert.cert')

    def test_cert_arg_to_installation_real_cert(self):
        self.copy_to_master(certifi.where())
        install_dir = '/opt'
        cert_file = os.path.basename(certifi.where())
        script = """
            set -e
            cp {mount_dir}/prestoadmin-*.tar.bz2 {install_dir}
            cd {install_dir}
            tar jxf prestoadmin-*.tar.bz2
            cd prestoadmin
             ./install-prestoadmin.sh {mount_dir}/{cacert}
        """.format(mount_dir=DOCKER_MOUNT_POINT, install_dir=install_dir,
                   cacert=cert_file)
        output = self.run_script(script)
        self.assertTrue('Adding pypi.python.org as trusted-host. Cannot find'
                        ' certificate file: %s' % cert_file not in output,
                        'Unable to find cert file; output: %s' % output)
