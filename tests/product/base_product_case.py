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

import fnmatch
import json
import re
import os
import shutil
import tempfile
import errno
import urllib

from docker import Client
from nose.tools import nottest

import prestoadmin
from tests.base_test_case import BaseTestCase
from tests.docker_cluster import DockerCluster, DockerClusterException, \
    INSTALLED_PRESTO_TEST_SLAVE_IMAGE, INSTALLED_PRESTO_TEST_MASTER_IMAGE, \
    LOCAL_MOUNT_POINT, DOCKER_MOUNT_POINT, LOCAL_RESOURCES_DIR

DIST_DIR = os.path.join(prestoadmin.main_dir, 'tmp/installer')

# TODO: make tests not dependent on the particular version of Presto at
# http://teradata-download.s3.amazonaws.com/aster/presto/lib/presto-0.101-1.0.x86_64.rpm
PRESTO_RPM = 'presto-0.101-1.0.x86_64.rpm'
PRESTO_RPM_BASENAME = r'presto-.*'
PRESTO_VERSION = r'presto-main:.*'


class BaseProductTestCase(BaseTestCase):
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
-XX:-UseBiasedLocking
-XX:+UseG1GC
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+UseGCOverheadLimit
-XX:OnOutOfMemoryError=kill -9 %p\n"""

    default_coordinator_config_ = """coordinator=true
discovery-server.enabled=true
discovery.uri=http://master:8080
http-server.http.port=8080
task.max-memory=1GB\n"""

    down_node_connection_error = r'(\nWarning: (\[%(host)s\] )?Low level socket ' \
                                 r'error connecting to host %(host)s on ' \
                                 r'port 22: No route to host ' \
                                 r'\(tried 1 time\)\n\nUnderlying ' \
                                 r'exception:\n    No route to host\n' \
                                 r'|\nWarning: (\[.*\] )?Timed out trying ' \
                                 r'to connect to %(host)s \(tried 1 ' \
                                 r'time\)\n\nUnderlying exception:' \
                                 r'\n    timed out\n)'
    len_down_node_error = 6

    def setUp(self):
        super(BaseProductTestCase, self).setUp()
        self.maxDiff = None
        self.docker_client = Client(timeout=180)

    def setup_docker_cluster(self, cluster_type='centos'):
        cluster_types = ['presto', 'centos']
        if cluster_type not in cluster_types:
            self.fail('{0} is not a supported cluster type. Must choose one'
                      ' from {1}'.format(cluster_type, cluster_types))

        try:
            are_presto_images_present = DockerCluster.check_for_presto_images()
            if cluster_type == 'presto' and are_presto_images_present:
                self.docker_cluster = DockerCluster.start_presto_cluster()
                return
            self.docker_cluster = DockerCluster.start_centos_cluster()
            if cluster_type == 'presto' and not are_presto_images_present:
                self.install_presto_admin()
                self.upload_topology()
                self.server_install()
                self.docker_client.commit(
                    self.docker_cluster.master,
                    INSTALLED_PRESTO_TEST_MASTER_IMAGE
                )
                self.docker_client.commit(
                    self.docker_cluster.slaves[0],
                    INSTALLED_PRESTO_TEST_SLAVE_IMAGE
                )
        except DockerClusterException as e:
            self.fail(e.msg)

    def tearDown(self):
        self.restore_stdout_stderr_keep_open()
        self.docker_cluster.tear_down_containers(LOCAL_MOUNT_POINT)

    def copy_to_host(self, source_path, dest_host):
        shutil.copy(source_path, os.path.join(LOCAL_MOUNT_POINT, dest_host))

    @nottest
    def clean_up_presto_test_images(self):
        try:
            self.docker_client.remove_image(
                INSTALLED_PRESTO_TEST_MASTER_IMAGE)
            self.docker_client.remove_image(
                INSTALLED_PRESTO_TEST_SLAVE_IMAGE)
        except:
            pass

    def build_dist_if_necessary(self):
        if not os.path.exists(DIST_DIR) or not fnmatch.filter(
                os.listdir(DIST_DIR), 'prestoadmin-*.tar.bz2'):
            self.clean_up_presto_test_images()
            self.build_installer_in_docker()
        return DIST_DIR

    def build_installer_in_docker(self):
        container_name = 'installer'
        root_local_mount_point = tempfile.mkdtemp()
        local_mount_point = os.path.join(root_local_mount_point, 'installer')
        installer_container = DockerCluster(container_name, [])
        try:
            installer_container.create_image(
                os.path.join(LOCAL_RESOURCES_DIR, 'centos6-ssh-test'),
                'teradatalabs/centos6-ssh-test',
                'jdeathe/centos-ssh'
            )
            installer_container.start_containers(
                root_local_mount_point,
                DOCKER_MOUNT_POINT,
                'teradatalabs/centos6-ssh-test'
            )
        except DockerClusterException as e:
            installer_container.tear_down_containers(root_local_mount_point)
            self.fail(e.msg)

        try:
            shutil.copytree(prestoadmin.main_dir,
                            os.path.join(local_mount_point, 'presto-admin'))
            local_path = os.path.join(local_mount_point, 'make_installer.sh')
            with open(local_path, 'w') as f:
                f.write('#!/bin/bash\n'
                        '-e\n'
                        'pip install --upgrade pip\n'
                        'pip install --upgrade wheel\n'
                        'pip install --upgrade setuptools\n'
                        'mv %s/presto-admin ~/\n'
                        'cd ~/presto-admin\n'
                        'make dist\n'
                        'cp dist/prestoadmin-*.tar.bz2 %s'
                        % (DOCKER_MOUNT_POINT, DOCKER_MOUNT_POINT))

            installer_container.exec_cmd_on_container(
                container_name,
                'chmod +x %s/make_installer.sh' % DOCKER_MOUNT_POINT)
            installer_container.exec_cmd_on_container(
                container_name,
                '%s/make_installer.sh' % DOCKER_MOUNT_POINT)

            try:
                os.makedirs(DIST_DIR)
            except OSError, e:
                if e.errno != errno.EEXIST:
                    raise
            installer_file = fnmatch.filter(os.listdir(local_mount_point),
                                            'prestoadmin-*.tar.bz2')[0]
            shutil.copy(os.path.join(local_mount_point, installer_file),
                        DIST_DIR)
        finally:
            installer_container.tear_down_containers(root_local_mount_point)

    def copy_dist_to_host(self, local_dist_dir, dest_host):
        for dist_file in os.listdir(local_dist_dir):
            if fnmatch.fnmatch(dist_file, "prestoadmin-*.tar.bz2"):
                self.copy_to_host(os.path.join(local_dist_dir, dist_file),
                                  dest_host)

    def install_presto_admin(self, host=None):
        # default args cannot reference self so we to initialize it like this
        if not host:
            host = self.docker_cluster.master
        dist_dir = self.build_dist_if_necessary()
        self.copy_dist_to_host(dist_dir, host)
        self.copy_to_host(LOCAL_RESOURCES_DIR + "/install-admin.sh", host)
        self.docker_cluster.exec_cmd_on_container(
            host, DOCKER_MOUNT_POINT + "/install-admin.sh")

    def dump_and_cp_topology(self, topology):
        local_container_mount_point = os.path.join(
            LOCAL_MOUNT_POINT, self.docker_cluster.master)
        with open(os.path.join(local_container_mount_point,
                               "config.json"), "w") as conf_file:
            json.dump(topology, conf_file)
        self.docker_cluster.exec_cmd_on_container(
            self.docker_cluster.master,
            "cp %s /etc/opt/prestoadmin/" %
            os.path.join(DOCKER_MOUNT_POINT, "config.json")
        )

    def upload_topology(self, topology=None):
        if not topology:
            topology = {"coordinator": "master",
                        "workers": ["slave1", "slave2", "slave3"]}
        self.dump_and_cp_topology(topology)

    def check_if_corrupted_rpm(self):
        self.docker_cluster.exec_cmd_on_container(
            self.docker_cluster.master, 'rpm -K --nosignature '
            + os.path.join(DOCKER_MOUNT_POINT, PRESTO_RPM))

    def copy_presto_rpm_to_master(self):
        rpm_path = os.path.join(prestoadmin.main_dir, PRESTO_RPM)
        if not os.path.exists(rpm_path):
            urllib.urlretrieve('http://teradata-download.s3.amazonaws.com/'
                               'aster/presto/lib/presto-0.101-1.0.x86_64.rpm',
                               rpm_path)
        self.copy_to_host(rpm_path, self.docker_cluster.master)
        self.check_if_corrupted_rpm()

    def server_install(self):
        self.copy_presto_rpm_to_master()
        cmd_output = self.run_prestoadmin(
            'server install ' + os.path.join(DOCKER_MOUNT_POINT, PRESTO_RPM))
        return cmd_output

    def run_prestoadmin(self, command, raise_error=True):
        return self.docker_cluster.exec_cmd_on_container(
            self.docker_cluster.master,
            "/opt/prestoadmin/presto-admin %s" % command,
            raise_error=raise_error
        )

    def run_prestoadmin_script(self, script_contents):
        temp_script = '/opt/prestoadmin/tmp.sh'
        self.write_content_to_docker_host(
            '#!/bin/bash\ncd /opt/prestoadmin\n%s' % script_contents,
            temp_script, self.docker_cluster.master)
        self.docker_cluster.exec_cmd_on_container(
            self.docker_cluster.master, 'chmod +x %s' % temp_script)
        return self.docker_cluster.exec_cmd_on_container(
            self.docker_cluster.master, temp_script)

    def run_script(self, script_contents, host):
        temp_script = '/tmp/tmp.sh'
        self.write_content_to_docker_host('#!/bin/bash\n%s' % script_contents,
                                          temp_script, host)
        self.docker_cluster.exec_cmd_on_container(
            host, 'chmod +x %s' % temp_script)
        return self.docker_cluster.exec_cmd_on_container(
            host, temp_script, tty=True)

    def get_ip_address_dict(self):
        ip_addresses = {}
        all_hosts = self.docker_cluster.all_hosts()
        for host in all_hosts:
            inspect = self.docker_client.inspect_container(host)
            ip_addresses[host] = inspect['NetworkSettings']['IPAddress']
        return ip_addresses

    def write_content_to_docker_host(self, content, path, host):
        filename = os.path.basename(path)
        dest_dir = os.path.dirname(path)
        host_local_mount_point = os.path.join(LOCAL_MOUNT_POINT,
                                              host)
        local_path = os.path.join(host_local_mount_point,
                                  filename)

        with open(local_path, 'w') as config_file:
            config_file.write(content)

        self.docker_cluster.exec_cmd_on_container(
            host, 'mkdir -p ' + dest_dir)
        self.docker_cluster.exec_cmd_on_container(
            host, 'cp %s %s' % (
                os.path.join(DOCKER_MOUNT_POINT, filename), dest_dir))

    def assert_path_exists(self, host, file_path):
        self.docker_cluster.exec_cmd_on_container(
            host, ' [ -e %s ] ' % file_path)

    def assert_file_content(self, host, filepath, expected):
        config = self.docker_cluster.exec_cmd_on_container(
            host, 'cat %s' % filepath)
        self.assertEqual(config, expected)

    def assert_file_content_regex(self, host, filepath, expected):
        config = self.docker_cluster.exec_cmd_on_container(
            host, 'cat %s' % filepath)
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
        self.docker_cluster.exec_cmd_on_container(
            container, ' [ ! -e %s ]' % directory)

    def assert_installed(self, container):
        check_rpm = self.docker_cluster.exec_cmd_on_container(
            container, 'rpm -q presto')
        self.assertEqual(PRESTO_RPM[:-4] + '\n', check_rpm)

    def assert_uninstalled(self, container):
        self.assertRaisesRegexp(OSError, 'package presto is not installed',
                                self.docker_cluster.exec_cmd_on_container,
                                container, 'rpm -q presto')

    def assert_has_default_config(self, container):
        self.assert_file_content(container,
                                 '/etc/presto/jvm.config',
                                 self.default_jvm_config_)

        self.assert_node_config(container, self.default_node_properties_)

        if container in self.docker_cluster.slaves:
            self.assert_file_content(container,
                                     '/etc/presto/config.properties',
                                     self.default_workers_config_)

        else:
            self.assert_file_content(container,
                                     '/etc/presto/config.properties',
                                     self.default_coordinator_config_)

    def assert_node_config(self, container, expected):
        node_properties = self.docker_cluster.exec_cmd_on_container(
            container, 'cat /etc/presto/node.properties')
        split_properties = node_properties.split('\n', 1)
        self.assertRegexpMatches(split_properties[0], 'node.id=.*')
        self.assertEqual(expected, split_properties[1])

    def expected_stop(self, running=None, not_running=None):
        if running is None:
            running = self.docker_cluster.all_hosts()
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
                                    self.docker_cluster.exec_cmd_on_container,
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

    def assert_started(self, process_per_host):
        for host, pid in process_per_host:
            self.docker_cluster.exec_cmd_on_container(host, 'kill -0 %s' %
                                                      pid)
        return process_per_host

    def escape_for_regex(self, expected):
        expected = expected.replace('{rpm}', PRESTO_RPM)
        expected = expected.replace('{rpm_basename}', PRESTO_RPM_BASENAME)
        expected = expected.replace('[', '\[')
        expected = expected.replace(']', '\]')
        expected = expected.replace(')', '\)')
        expected = expected.replace('(', '\(')
        expected = expected.replace('+', '\+')
        return expected
