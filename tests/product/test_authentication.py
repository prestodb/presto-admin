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
Product tests for SSH authentication for presto-admin commands
"""

import os
import subprocess
import re

from nose.plugins.attrib import attr

from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase, docker_only
from tests.product.cluster_types import STANDALONE_PRESTO_CLUSTER
from constants import LOCAL_RESOURCES_DIR
from tests.product.config_dir_utils import get_catalog_directory, get_presto_admin_path


class TestAuthentication(BaseProductTestCase):
    def setUp(self):
        super(TestAuthentication, self).setUp()
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PRESTO_CLUSTER)

    success_output = (
        'Deploying tpch.properties catalog configurations on: slave1 \n'
        'Deploying tpch.properties catalog configurations on: master \n'
        'Deploying tpch.properties catalog configurations on: slave2 \n'
        'Deploying tpch.properties catalog configurations on: slave3 \n'
    )

    interactive_text = (
        '/usr/lib64/python2.6/getpass.py:83: GetPassWarning: Can not control '
        'echo on the terminal.\n'
        'Initial value for env.password: \n'
        'Warning: Password input may be echoed.\n'
        '  passwd = fallback_getpass(prompt, stream)\n'
    )

    sudo_password_prompt = (
        '[master] out: sudo password:\n'
        '[master] out: \n'
        '[slave1] out: sudo password:\n'
        '[slave1] out: \n'
        '[slave2] out: sudo password:\n'
        '[slave2] out: \n'
        '[slave3] out: sudo password:\n'
        '[slave3] out: \n'
    )

    def parallel_password_failure_message(self, with_sudo_prompt=True):
        with open(os.path.join(LOCAL_RESOURCES_DIR,
                               'parallel_password_failure.txt')) as f:
            parallel_password_failure = f.read()
        if with_sudo_prompt:
            parallel_password_failure += (
                '[%(slave3)s] out: sudo password:\n'
                '[%(slave3)s] out: Sorry, try again.\n'
                '[%(slave2)s] out: sudo password:\n'
                '[%(slave2)s] out: Sorry, try again.\n'
                '[%(slave1)s] out: sudo password:\n'
                '[%(slave1)s] out: Sorry, try again.\n'
                '[%(master)s] out: sudo password:\n'
                '[%(master)s] out: Sorry, try again.\n')
        parallel_password_failure = parallel_password_failure % {
            'master': self.cluster.internal_master,
            'slave1': self.cluster.internal_slaves[0],
            'slave2': self.cluster.internal_slaves[1],
            'slave3': self.cluster.internal_slaves[2]}
        return parallel_password_failure

    def non_root_sudo_warning_message(self):
        with open(os.path.join(LOCAL_RESOURCES_DIR,
                               'non_root_sudo_warning_text.txt')) as f:
            non_root_sudo_warning = f.read()
        return non_root_sudo_warning

    @attr('smoketest')
    @docker_only
    def test_passwordless_ssh_authentication(self):
        self.upload_topology()
        self.setup_for_catalog_add()

        # Passwordless SSH as root, but specify -I
        # We need to do it as a script because docker_py doesn't support
        # redirecting stdin.
        command_output = self.run_script_from_prestoadmin_dir(
            'echo "password" | ./presto-admin catalog add -I')

        self.assertEqualIgnoringOrder(
            self._remove_python_string(self.success_output + self.interactive_text),
            self._remove_python_string(command_output))

        # Passwordless SSH as root, but specify -p
        command_output = self.run_prestoadmin('catalog add --password '
                                              'password')
        self.assertEqualIgnoringOrder(self.success_output, command_output)

        # Passwordless SSH as app-admin, specify -I
        non_root_sudo_warning = self.non_root_sudo_warning_message()

        command_output = self.run_script_from_prestoadmin_dir(
            'echo "password" | ./presto-admin catalog add -I -u app-admin')
        self.assertEqualIgnoringOrder(
            self._remove_python_string(
                self.success_output + self.interactive_text +
                self.sudo_password_prompt + non_root_sudo_warning),
            self._remove_python_string(command_output))

        # Passwordless SSH as app-admin, but specify -p
        command_output = self.run_prestoadmin('catalog add --password '
                                              'password -u app-admin')
        self.assertEqualIgnoringOrder(
            self.success_output + self.sudo_password_prompt +
            self.sudo_password_prompt, command_output)

        # Passwordless SSH as app-admin, but specify wrong password with -I
        parallel_password_failure = self.parallel_password_failure_message()
        command_output = self.run_script_from_prestoadmin_dir(
            'echo "asdf" | ./presto-admin catalog add -I -u app-admin',
            raise_error=False)
        self.assertEqualIgnoringOrder(
            self._remove_python_string(parallel_password_failure + self.interactive_text),
            self._remove_python_string(command_output))

        # Passwordless SSH as app-admin, but specify wrong password with -p
        command_output = self.run_prestoadmin(
            'catalog add --password asdf -u app-admin', raise_error=False)
        self.assertEqualIgnoringOrder(parallel_password_failure,
                                      command_output)

        # Passwordless SSH as root, in serial mode
        command_output = self.run_script_from_prestoadmin_dir(
            './presto-admin catalog add --serial')
        self.assertEqualIgnoringOrder(
            self.success_output, command_output)

    @attr('smoketest')
    @docker_only
    def test_no_passwordless_ssh_authentication(self):
        self.upload_topology()
        self.setup_for_catalog_add()

        # This is needed because the test for
        # No passwordless SSH, -I correct -u app-admin,
        # was giving Device not a stream error in jenkins
        self.run_script_from_prestoadmin_dir(
            'echo "password" | ./presto-admin catalog add -I')

        for host in self.cluster.all_hosts():
            self.cluster.exec_cmd_on_host(
                host,
                'mv /root/.ssh/id_rsa /root/.ssh/id_rsa.bak'
            )

        # No passwordless SSH, no -I or -p
        parallel_password_failure = self.parallel_password_failure_message(
            with_sudo_prompt=False)
        command_output = self.run_prestoadmin(
            'catalog add', raise_error=False)
        self.assertEqualIgnoringOrder(parallel_password_failure,
                                      command_output)

        # No passwordless SSH, -p incorrect -u root
        command_output = self.run_prestoadmin(
            'catalog add --password password', raise_error=False)
        self.assertEqualIgnoringOrder(parallel_password_failure,
                                      command_output)

        # No passwordless SSH, -I correct -u app-admin
        non_root_sudo_warning = self.non_root_sudo_warning_message()
        command_output = self.run_script_from_prestoadmin_dir(
            'echo "password" | ./presto-admin catalog add -I -u app-admin')
        self.assertEqualIgnoringOrder(
            self._remove_python_string(
                self.success_output + self.interactive_text +
                self.sudo_password_prompt + non_root_sudo_warning),
            self._remove_python_string(command_output))

        # No passwordless SSH, -p correct -u app-admin
        command_output = self.run_prestoadmin('catalog add -p password '
                                              '-u app-admin')
        self.assertEqualIgnoringOrder(
            self.success_output + self.sudo_password_prompt +
            self.sudo_password_prompt, command_output)

        # No passwordless SSH, specify keyfile with -i
        self.cluster.exec_cmd_on_host(
            self.cluster.master, 'chmod 600 /root/.ssh/id_rsa.bak')
        command_output = self.run_prestoadmin(
            'catalog add -i /root/.ssh/id_rsa.bak')
        self.assertEqualIgnoringOrder(self.success_output, command_output)

        for host in self.cluster.all_hosts():
            self.cluster.exec_cmd_on_host(
                host,
                'mv /root/.ssh/id_rsa.bak /root/.ssh/id_rsa'
            )

    @attr('smoketest', 'quarantine')
    @docker_only
    def test_prestoadmin_no_sudo_popen(self):
        self.upload_topology()
        self.setup_for_catalog_add()

        # We use Popen because docker-py loses the first 8 characters of TTY
        # output.
        args = ['docker', 'exec', '-t', self.cluster.master, 'sudo',
                '-u', 'app-admin', get_presto_admin_path(),
                'topology show']
        proc = subprocess.Popen(args, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
        self.assertRegexpMatchesLineByLine(
            'Please run presto-admin with sudo.\n'
            '\\[Errno 13\\] Permission denied: \'.*/.prestoadmin/log'
            'presto-admin.log\'', proc.stdout.read())

    def setup_for_catalog_add(self):
        connector_script = 'mkdir -p %(catalogs)s\n' \
                           'echo \'connector.name=tpch\' >> %(catalogs)s/tpch.properties\n' % \
                           {'catalogs': get_catalog_directory()}
        self.run_script_from_prestoadmin_dir(connector_script)

    def _remove_python_string(self, text):
        return re.sub(r'python2\.6|python2\.7', '', text)
