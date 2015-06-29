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
import errno
import urllib

from docker import Client

import prestoadmin
from tests.base_test_case import BaseTestCase
from tests.docker_cluster import DockerCluster, DockerClusterException, \
    INSTALLED_PRESTO_TEST_SLAVE_IMAGE, INSTALLED_PRESTO_TEST_MASTER_IMAGE, \
    LOCAL_RESOURCES_DIR, DEFAULT_LOCAL_MOUNT_POINT, DEFAULT_DOCKER_MOUNT_POINT

PRESTO_RPM_GLOB = r'presto-*.x86_64.rpm'
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
                                 r'|\nWarning: (\[%(host)s] )?Timed out trying ' \
                                 r'to connect to %(host)s \(tried 1 ' \
                                 r'time\)\n\nUnderlying exception:' \
                                 r'\n    timed out\n)'
    len_down_node_error = 6

    def setUp(self):
        super(BaseProductTestCase, self).setUp()
        self.maxDiff = None
        self.docker_client = Client(timeout=180)
        self.presto_rpm_filename = self.detect_presto_rpm()

    def detect_presto_rpm(self):
        """
        Detects the Presto RPM in the main directory of presto-admin.
        Returns the name of the RPM, if it exists, else returns None.
        """
        rpm_names = fnmatch.filter(os.listdir(prestoadmin.main_dir),
                                   PRESTO_RPM_GLOB)
        if rpm_names:
            # Choose the last RPM name if you sort the list, since if there
            # are multiple RPMs, the last one is probably the latest
            return sorted(rpm_names)[-1]
        else:
            return None

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
                self.install_presto_admin(self.docker_cluster)
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
        if hasattr(locals()['self'], 'docker_cluster'):
            self.docker_cluster.tear_down_containers()

    def build_dist_if_necessary(self, cluster=None, unique=False):
        if not cluster:
            cluster = self.docker_cluster
        if (not os.path.isdir(cluster.get_dist_dir(unique)) or
            not fnmatch.filter(
                os.listdir(cluster.get_dist_dir(unique)),
                'prestoadmin-*.tar.bz2')):
            cluster.clean_up_presto_test_images()
            self.build_installer_in_docker(cluster=cluster, unique=unique)
        return cluster.get_dist_dir(unique)

    def build_installer_in_docker(self, online_installer=False, cluster=None,
                                  unique=False):
        if not cluster:
            cluster = self.docker_cluster
        container_name = 'installer'
        installer_container = DockerCluster(
            container_name, [], DEFAULT_LOCAL_MOUNT_POINT,
            DEFAULT_DOCKER_MOUNT_POINT)
        try:
            installer_container.create_image(
                os.path.join(LOCAL_RESOURCES_DIR, 'centos6-ssh-test'),
                'teradatalabs/centos6-ssh-test',
                'jdeathe/centos-ssh'
            )
            installer_container.start_containers(
                'teradatalabs/centos6-ssh-test'
            )
        except DockerClusterException as e:
            installer_container.tear_down_containers()
            self.fail(e.msg)

        try:
            shutil.copytree(
                prestoadmin.main_dir,
                os.path.join(
                    installer_container.get_local_mount_dir(container_name),
                    'presto-admin'),
                ignore=shutil.ignore_patterns('tmp', '.git', 'presto*.rpm')
            )
            installer_container.run_script(
                '-e\n'
                'pip install --upgrade pip\n'
                'pip install --upgrade wheel\n'
                'pip install --upgrade setuptools\n'
                'mv %s/presto-admin ~/\n'
                'cd ~/presto-admin\n'
                'make %s\n'
                'cp dist/prestoadmin-*.tar.bz2 %s'
                % (installer_container.docker_mount_dir,
                   'dist' if not online_installer else 'dist-online',
                   installer_container.docker_mount_dir),
                container_name)

            try:
                os.makedirs(cluster.get_dist_dir(unique))
            except OSError, e:
                if e.errno != errno.EEXIST:
                    raise
            local_container_dist_dir = os.path.join(
                prestoadmin.main_dir,
                installer_container.get_local_mount_dir(container_name)
            )
            installer_file = fnmatch.filter(
                os.listdir(local_container_dist_dir),
                'prestoadmin-*.tar.bz2')[0]
            shutil.copy(
                os.path.join(local_container_dist_dir, installer_file),
                cluster.get_dist_dir(unique))
        finally:
            installer_container.tear_down_containers()

    def copy_dist_to_host(self, local_dist_dir, dest_host, cluster=None):
        if not cluster:
            cluster = self.docker_cluster
        for dist_file in os.listdir(local_dist_dir):
            if fnmatch.fnmatch(dist_file, "prestoadmin-*.tar.bz2"):
                cluster.copy_to_host(
                    os.path.join(local_dist_dir, dist_file),
                    dest_host)

    def install_presto_admin(self, cluster, dist_dir=None):
        if not dist_dir:
            dist_dir = self.build_dist_if_necessary(cluster=cluster)
        self.copy_dist_to_host(dist_dir, cluster.master, cluster)
        cluster.copy_to_host(
            LOCAL_RESOURCES_DIR + "/install-admin.sh", cluster.master)
        cluster.exec_cmd_on_container(
            cluster.master, cluster.docker_mount_dir + "/install-admin.sh")

    def dump_and_cp_topology(self, topology):
        local_container_mount_point = self.docker_cluster.get_local_mount_dir(
            self.docker_cluster.master)
        with open(os.path.join(local_container_mount_point,
                               "config.json"), "w") as conf_file:
            json.dump(topology, conf_file)
        self.docker_cluster.exec_cmd_on_container(
            self.docker_cluster.master,
            "cp %s /etc/opt/prestoadmin/" %
            os.path.join(self.docker_cluster.docker_mount_dir, "config.json")
        )

    def upload_topology(self, topology=None):
        if not topology:
            topology = {"coordinator": "master",
                        "workers": ["slave1", "slave2", "slave3"]}
        self.dump_and_cp_topology(topology)

    def check_if_corrupted_rpm(self):
        self.docker_cluster.exec_cmd_on_container(
            self.docker_cluster.master, 'rpm -K --nosignature '
            + os.path.join(self.docker_cluster.docker_mount_dir,
                           self.presto_rpm_filename))

    def copy_presto_rpm_to_master(self):
        if not self.presto_rpm_filename:
            # TODO: once the RPM is on Maven Central, pull the RPM from there
            self.presto_rpm_filename = 'presto-0.101-1.0.x86_64.rpm'
            rpm_path = os.path.join(prestoadmin.main_dir,
                                    self.presto_rpm_filename)
            urllib.urlretrieve('http://teradata-download.s3.amazonaws.com/'
                               'aster/presto/lib/presto-0.101-1.0.x86_64.rpm',
                               rpm_path)
        else:
            rpm_path = os.path.join(prestoadmin.main_dir,
                                    self.presto_rpm_filename)
        self.docker_cluster.copy_to_host(rpm_path, self.docker_cluster.master)
        self.check_if_corrupted_rpm()

    def server_install(self):
        self.copy_presto_rpm_to_master()
        cmd_output = self.run_prestoadmin(
            'server install ' + os.path.join(
                self.docker_cluster.docker_mount_dir,
                self.presto_rpm_filename))
        return cmd_output

    def run_prestoadmin(self, command, raise_error=True, cluster=None):
        if not cluster:
            cluster = self.docker_cluster
        command = self.replace_keywords(command, cluster=cluster)
        return cluster.exec_cmd_on_container(
            cluster.master,
            "/opt/prestoadmin/presto-admin %s" % command,
            raise_error=raise_error
        )

    def run_prestoadmin_script(self, script_contents, **kwargs):
        script_contents = self.replace_keywords(script_contents,
                                                **kwargs)
        temp_script = '/opt/prestoadmin/tmp.sh'
        self.docker_cluster.write_content_to_docker_host(
            '#!/bin/bash\ncd /opt/prestoadmin\n%s' % script_contents,
            temp_script, self.docker_cluster.master)
        self.docker_cluster.exec_cmd_on_container(
            self.docker_cluster.master, 'chmod +x %s' % temp_script)
        return self.docker_cluster.exec_cmd_on_container(
            self.docker_cluster.master, temp_script)

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
        self.assertEqual(self.presto_rpm_filename[:-4] + '\n', check_rpm)

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
            running = self.docker_cluster.all_internal_hosts()
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

    def replace_keywords(self, text, cluster=None, **kwargs):
        if not cluster:
            cluster = self.docker_cluster
        test_keywords = {
            'rpm': self.presto_rpm_filename,
            'rpm_basename': self.presto_rpm_filename[:-4],
            'rpm_basename_without_arch': self.presto_rpm_filename[:-11],
            'master': cluster.internal_master
        }
        if cluster.internal_slaves:
            test_keywords.update({
                'slave1': cluster.internal_slaves[0],
                'slave2': cluster.internal_slaves[1],
                'slave3': cluster.internal_slaves[2]
            })
        test_keywords.update(**kwargs)
        return text % test_keywords

    def escape_for_regex(self, expected):
        expected = expected.replace('[', '\[')
        expected = expected.replace(']', '\]')
        expected = expected.replace(')', '\)')
        expected = expected.replace('(', '\(')
        expected = expected.replace('+', '\+')
        return expected
