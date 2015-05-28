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
Base class for product tests.  Handles setting up a docker cluster and has
other utilities
"""

import distutils.core
import fnmatch
import json
import re
import os
import shutil
import subprocess
import sys
from time import sleep
import errno
import urllib

from docker.errors import APIError
from docker import Client

import prestoadmin
from tests import utils

DOCKER_START_TIMEOUT = 20

INSTALLED_PRESTO_TEST_MASTER_IMAGE = 'teradatalabs/centos-presto-test-master'
INSTALLED_PRESTO_TEST_SLAVE_IMAGE = 'teradatalabs/centos-presto-test-slave'

LOCAL_MOUNT_POINT = os.path.join(prestoadmin.main_dir, "tmp/docker-pa/%s")
LOCAL_RESOURCES_DIR = os.path.join(prestoadmin.main_dir,
                                   "tests/product/resources/")
DOCKER_MOUNT_POINT = "/mnt/presto-admin"
PRESTO_RPM = 'presto-0.101-1.0.x86_64.rpm'
PRESTO_VERSION = 'presto-main:0.101-SNAPSHOT'


class BaseProductTestCase(utils.BaseTestCase):
    default_workers_config_ = """coordinator=false
discovery.uri=http://master:8080
http-server.http.port=8080
task.max-memory=1GB\n"""
    default_node_properties_ = """node.data-dir=/var/lib/presto/data
node.environment=presto
plugin.config-dir=/etc/presto/catalog
plugin.dir=/usr/lib/presto/lib/plugin\n"""

    default_jvm_config_ = """-server
-Xmx1G
-XX:+UseConcMarkSweepGC
-XX:+ExplicitGCInvokesConcurrent
-XX:+CMSClassUnloadingEnabled
-XX:+AggressiveOpts
-XX:+HeapDumpOnOutOfMemoryError
-XX:OnOutOfMemoryError=kill -9 %p
-XX:ReservedCodeCacheSize=150M\n"""

    default_coordinator_config_ = """coordinator=true
discovery-server.enabled=true
discovery.uri=http://master:8080
http-server.http.port=8080
task.max-memory=1GB\n"""

    client = Client(timeout=180)
    slaves = ["slave1", "slave2", "slave3"]
    master = "master"

    down_node_connection_error = r'(Low level socket error connecting to ' \
                                 r'host %(host)s on port 22: No route to ' \
                                 r'host \(tried 1 time\)|Timed out trying ' \
                                 r'to connect to %(host)s \(tried 1 time\))'
    serial_down_node_connection_error = r'(\nWarning: Low level socket error ' \
                                        r'connecting to host %(host)s on ' \
                                        r'port 22: No route to host ' \
                                        r'\(tried 1 time\)\n\nUnderlying ' \
                                        r'exception:\n    No route to host\n' \
                                        r'|\nWarning: Timed out trying to ' \
                                        r'connect to %(host)s \(tried 1 ' \
                                        r'time\)\n\nUnderlying exception:' \
                                        r'\n    timed out\n)'

    def setUp(self):
        self.maxDiff = None
        self.check_if_docker_exists()
        self.create_docker_cluster()
        self.capture_stdout_stderr()

    def tearDown(self):
        self.restore_stdout_stderr_keep_open()
        self.tear_down_docker_cluster()

    def check_if_docker_exists(self):
        try:
            subprocess.call(['docker', '--version'])
        except OSError:
            sys.exit('Docker is not installed. Try installing it with '
                     'presto-admin/bin/install-docker.sh.')

    def create_host_mount_dirs(self):
        for container_name in self.all_hosts():
            try:
                os.makedirs(LOCAL_MOUNT_POINT % container_name)
            except OSError as e:
                # file exists
                if e.errno == errno.EEXIST:
                    pass
                else:
                    raise

    def create_and_start_containers(self, master_image=None, slave_image=None):
        if not master_image:
            master_image = 'teradatalabs/centos6-ssh-test'

        if not slave_image:
            slave_image = 'teradatalabs/centos6-ssh-test'

        for container_name in self.slaves:
            self._execute_and_wait(self.client.create_container,
                                   slave_image,
                                   detach=True,
                                   name=container_name,
                                   volumes=LOCAL_MOUNT_POINT %
                                   container_name)

            self.client.start(container_name,
                              binds={LOCAL_MOUNT_POINT % container_name:
                                     {"bind": DOCKER_MOUNT_POINT,
                                      "ro": False}})
        self._execute_and_wait(self.client.create_container,
                               master_image,
                               detach=True,
                               name=self.master,
                               hostname=self.master,
                               volumes=LOCAL_MOUNT_POINT % self.master)
        self.client.start(self.master,
                          binds={LOCAL_MOUNT_POINT % self.master:
                                 {"bind": DOCKER_MOUNT_POINT,
                                  "ro": False}},
                          links=zip(self.slaves, self.slaves))

    def create_docker_cluster(self):
        self.tear_down_docker_cluster()
        self.create_host_mount_dirs()

        if not self.client.images('jdeathe/centos-ssh'):
            self._execute_and_wait(self.client.pull, 'jdeathe/centos-ssh')

        self._execute_and_wait(self.client.build,
                               path=os.path.join(prestoadmin.main_dir,
                                                 'tests/product/resources/'
                                                 'centos6-ssh-test'),
                               tag='teradatalabs/centos6-ssh-test', rm=True)

        self.create_and_start_containers()
        self.ensure_docker_containers_started()

    def _execute_and_wait(self, func, *args, **kwargs):
        ret = func(*args, **kwargs)
        # go through all lines in returned stream to ensure func finishes
        for line in ret:
            pass

    def remove_host_mount_dirs(self):
        for container_name in self.all_hosts():
            try:
                shutil.rmtree(LOCAL_MOUNT_POINT % container_name)
            except OSError as e:
                # no such file or directory
                if e.errno == errno.ENOENT:
                    pass

        try:
            os.removedirs(os.path.dirname(LOCAL_MOUNT_POINT))
        except OSError as e:
            if e.errno == errno.ENOENT:
                pass

    def tear_down_docker_cluster(self):
        for container in self.all_hosts():
            try:
                self.stop_and_wait(container)
                self.client.remove_container(container, v=True)
            except APIError as e:
                # container does not exist
                if e.response.status_code == 404:
                    pass

        self.remove_host_mount_dirs()

    def stop_and_wait(self, container):
        self.client.stop(container)
        self.client.wait(container)

    def copy_to_master(self, source):
        shutil.copy(source, LOCAL_MOUNT_POINT % self.master)

    def clean_up_presto_test_images(self):
        try:
            self.client.remove_image(INSTALLED_PRESTO_TEST_MASTER_IMAGE)
            self.client.remove_image(INSTALLED_PRESTO_TEST_SLAVE_IMAGE)
        except:
            pass

    def build_dist_if_necessary(self):
        dist_dir = os.path.join(prestoadmin.main_dir, "dist")
        if not os.path.exists(dist_dir) or not fnmatch.filter(
                os.listdir(dist_dir), "prestoadmin-*.tar.bz2"):
            self.clean_up_presto_test_images()
            # setup.py expects you to be in the main directory
            saved_path = os.getcwd()
            os.chdir(prestoadmin.main_dir)
            distutils.core.run_setup("setup.py",
                                     ["bdist_prestoadmin"]).run_commands()
            os.chdir(saved_path)
        return dist_dir

    def copy_dist_to_master(self, dist_dir):
        for dist_file in os.listdir(dist_dir):
            if fnmatch.fnmatch(dist_file, "prestoadmin-*.tar.bz2"):
                self.copy_to_master(os.path.join(dist_dir, dist_file))

    def install_presto_admin(self):
        dist_dir = self.build_dist_if_necessary()
        self.copy_dist_to_master(dist_dir)
        self.copy_to_master(LOCAL_RESOURCES_DIR + "/install-admin.sh")
        self.exec_create_start(self.master,
                               DOCKER_MOUNT_POINT + "/install-admin.sh")

    def exec_create_start(self, host, command, raise_error=True, tty=False):
        ex = self.client.exec_create(host, command, tty=tty)
        output = self.client.exec_start(ex['Id'], tty=tty)
        exit_code = self.client.exec_inspect(ex['Id'])['ExitCode']
        if raise_error and exit_code:
            raise OSError(output)

        return output

    def dump_and_cp_topology(self, topology):
        with open(os.path.join(LOCAL_MOUNT_POINT % self.master,
                               "config.json"), "w") as conf_file:
            json.dump(topology, conf_file)
        self.exec_create_start(self.master, "cp %s /etc/opt/prestoadmin/" %
                               os.path.join(DOCKER_MOUNT_POINT, "config.json"))

    def upload_topology(self, topology=None):
        if not topology:
            topology = {"coordinator": "master",
                        "workers": ["slave1", "slave2", "slave3"]}
        self.dump_and_cp_topology(topology)

    def check_if_corrupted_rpm(self):
        self.exec_create_start(self.master, 'rpm -K --nosignature '
                               + os.path.join(DOCKER_MOUNT_POINT, PRESTO_RPM))

    def copy_presto_rpm_to_master(self):
        if not os.path.exists(PRESTO_RPM):
            urllib.urlretrieve(
                'https://jenkins-master.td.teradata.com/view/Presto/job/'
                'presto-td/lastSuccessfulBuild/artifact/presto-server/target'
                '/rpm/presto/RPMS/x86_64/%s' % PRESTO_RPM,
                PRESTO_RPM)
        self.copy_to_master(PRESTO_RPM)
        self.check_if_corrupted_rpm()

    def server_install(self):
        self.copy_presto_rpm_to_master()
        cmd_output = self.run_prestoadmin(
            'server install ' + os.path.join(DOCKER_MOUNT_POINT, PRESTO_RPM))
        return cmd_output

    def run_prestoadmin(self, command, raise_error=True):
        return self.exec_create_start(self.master,
                                      "/opt/prestoadmin/presto-admin %s"
                                      % command, raise_error=raise_error)

    def run_prestoadmin_script(self, script_contents):
        temp_script = '/opt/prestoadmin/tmp.sh'
        self.write_content_to_master('#!/bin/bash\ncd /opt/prestoadmin\n%s'
                                     % script_contents, temp_script)
        self.exec_create_start(self.master, 'chmod +x %s' % temp_script)
        return self.exec_create_start(self.master, temp_script)

    def run_script(self, script_contents):
        temp_script = '/tmp/tmp.sh'
        self.write_content_to_master('#!/bin/bash\n%s' % script_contents,
                                     temp_script)
        self.exec_create_start(self.master, 'chmod +x %s' % temp_script)
        return self.exec_create_start(self.master, temp_script, tty=True)

    def all_hosts(self):
        return self.slaves[:] + [self.master]

    def get_ip_address_dict(self):
        ip_addresses = {}
        for host in self.all_hosts():
            inspect = self.client.inspect_container(host)
            ip_addresses[host] = inspect['NetworkSettings']['IPAddress']
        return ip_addresses

    def write_content_to_master(self, content, path):
        # write to master
        filename = os.path.basename(path)
        dest_dir = os.path.dirname(path)
        local_path = os.path.join(LOCAL_MOUNT_POINT % self.master,
                                  filename)

        with open(local_path, 'w') as config_file:
            config_file.write(content)

        self.exec_create_start(self.master, 'mkdir -p ' + dest_dir)
        self.exec_create_start(self.master, 'cp %s %s' % (
            os.path.join(DOCKER_MOUNT_POINT, filename), dest_dir))

    def assert_file_content(self, host, filepath, expected):
        config = self.exec_create_start(host, 'cat %s' % filepath)
        self.assertEqual(config, expected)

    def assert_file_content_regex(self, host, filepath, expected):
        config = self.exec_create_start(host, 'cat %s' % filepath)
        self.assertRegexpMatches(config, expected)

    def assert_has_default_connector(self, container):
        self.assert_file_content(container,
                                 '/etc/presto/catalog/tpch.properties',
                                 'connector.name=tpch')

    def assert_has_jmx_connector(self, container):
        self.assert_file_content(container,
                                 '/etc/presto/catalog/jmx.properties',
                                 'connector.name=jmx')

    def assert_path_removed(self, container, directory):
        self.exec_create_start(container, ' [ ! -e %s ]' % directory)

    def assert_parallel_execution_failure(self, hosts, task,
                                          underlying_exception, cmd_output):
        expected_parallel_exception = "!!! Parallel execution exception " \
                                      "under host u'%s'"
        expected_stacktrace = 'Process %s:'

        for host in hosts:
            self.assertTrue(expected_parallel_exception % host in cmd_output,
                            "expected: %s\n output: %s\n"
                            % (expected_parallel_exception % host, cmd_output))
            self.assertTrue(expected_stacktrace % host in cmd_output,
                            "expected: %s\n output: %s\n"
                            % (expected_stacktrace % host, cmd_output))

        warning = """\nWarning: One or more hosts failed while executing task \
'%(task)s'

Underlying exception:
    %(exception)s\n
"""
        warning = warning % {'exception': underlying_exception,
                             'task': task}
        expected_warning = ''
        for i in range(len(hosts)):
            expected_warning += warning

        self.assertRegexpMatches(cmd_output, expected_warning)

    def assert_installed(self, container):
        check_rpm = self.exec_create_start(container,
                                           'rpm -q presto')
        self.assertEqual(PRESTO_RPM[:-4] + '\n', check_rpm)

    def assert_uninstalled(self, container):
        self.assertRaisesRegexp(OSError, 'package presto is not installed',
                                self.exec_create_start,
                                container, 'rpm -q presto')

    def assert_has_default_config(self, container):
        self.assert_file_content(container,
                                 '/etc/presto/jvm.config',
                                 self.default_jvm_config_)

        self.assert_node_config(container, self.default_node_properties_)

        if container in self.slaves:
            self.assert_file_content(container,
                                     '/etc/presto/config.properties',
                                     self.default_workers_config_)

        else:
            self.assert_file_content(container,
                                     '/etc/presto/config.properties',
                                     self.default_coordinator_config_)

    def assert_node_config(self, container, expected):
        node_properties = self.exec_create_start(
            container, 'cat /etc/presto/node.properties')
        split_properties = node_properties.split('\n', 1)
        self.assertRegexpMatches(split_properties[0], 'node.id=.*')
        self.assertEqual(expected, split_properties[1])

    def expected_stop(self, running=None, not_running=None):
        if running is None:
            running = self.all_hosts()
            if not_running:
                for host in not_running:
                    running.remove(host)

        expected_output = []
        for host in running:
            expected_output += [r'\[%s\] out: ' % host,
                                r'\[%s\] out: Stopped .*' % host,
                                r'\[%s\] out: Stopping presto' % host]
        if not_running:
            for host in not_running:
                expected_output += [r'\[%s\] out: ' % host,
                                    r'\[%s\] out: Not runnning' % host,
                                    r'\[%s\] out: Stopping presto' % host]

        return expected_output

    def assert_stopped(self, process_per_host):
        for host, pid in process_per_host:
            self.assertRaisesRegexp(OSError,
                                    'No such process',
                                    self.exec_create_start,
                                    host, 'kill -0 %s' % pid)

    def get_process_per_host(self, output_lines):
        process_per_host = []
        for line in output_lines:
            match = re.search(r'\[(?P<host>.*?)\] out: Started as (?P<pid>.*)',
                              line)
            if match:
                process_per_host.append((match.group('host'),
                                         match.group('pid')))
        return process_per_host

    def ensure_docker_containers_started(self):
        timeout = 0
        ps_output = ''
        while timeout < DOCKER_START_TIMEOUT:
            started = True
            for host in self.all_hosts():
                ps_output = self.exec_create_start(host, 'ps')
                # also ensure that the app-admin user exists
                try:
                    user_output = self.exec_create_start(
                        host, 'grep app-admin /etc/passwd')
                except OSError:
                    user_output = ''
                if 'sshd_bootstrap' in ps_output or 'sshd\n' not in ps_output \
                        or not user_output:
                    timeout += 1
                    started = False
            if not started:
                timeout += 1
                sleep(1)
            else:
                break
        if timeout is DOCKER_START_TIMEOUT:
            log = self.client.logs(self.all_hosts()[-1])
            self.fail('Docker container with presto timed out on start; '
                      'ps output: %s\n log output: %s\n' % (ps_output, log))

    def install_default_presto(self):
        """
        Installs default Presto on the docker cluster. If there is already
        a Docker image with Presto installed, use those Docker images. Else,
        perform the installation and then take a snapshot.

        This method must only be called on clean containers, else you'll
        get extra state for subsequent tests if it's the first time that
        install_default_presto has been called.
        """
        images = self.client.images()
        has_master = False
        has_slave = False
        for image in images:
            if INSTALLED_PRESTO_TEST_MASTER_IMAGE in image['RepoTags'][0]:
                has_master = True
            if INSTALLED_PRESTO_TEST_SLAVE_IMAGE in image['RepoTags'][0]:
                has_slave = True

        if has_master and has_slave:
            self.tear_down_docker_cluster()
            self.create_host_mount_dirs()
            self.create_and_start_containers(
                INSTALLED_PRESTO_TEST_MASTER_IMAGE,
                INSTALLED_PRESTO_TEST_SLAVE_IMAGE
            )

            self.ensure_docker_containers_started()
            return

        self.install_presto_admin()
        self.upload_topology()
        self.server_install()
        self.client.commit(self.master, INSTALLED_PRESTO_TEST_MASTER_IMAGE)
        self.client.commit(self.slaves[0], INSTALLED_PRESTO_TEST_SLAVE_IMAGE)

    def assert_started(self, process_per_host):
        for host, pid in process_per_host:
            self.exec_create_start(host, 'kill -0 %s' %
                                   pid)
        return process_per_host
