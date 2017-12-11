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

import json
import os
import re
from StringIO import StringIO

from nose.tools import nottest
from retrying import Retrying

from prestoadmin.prestoclient import PrestoClient
from prestoadmin.util import constants
from prestoadmin.util.constants import CONFIG_PROPERTIES, COORDINATOR_DIR_NAME, LOCAL_CONF_DIR
from prestoadmin.util.presto_config import PrestoConfig
from tests.base_test_case import BaseTestCase
from tests.configurable_cluster import ConfigurableCluster
from tests.docker_cluster import DockerCluster
from tests.product.cluster_types import cluster_types
from tests.product.config_dir_utils import get_coordinator_directory, get_workers_directory, get_config_file_path, \
    get_log_directory, get_install_directory, get_presto_admin_path
from tests.product.standalone.presto_installer import StandalonePrestoInstaller

PRESTO_VERSION = r'.+'
RETRY_TIMEOUT = 120
RETRY_INTERVAL = 5


class BaseProductTestCase(BaseTestCase):
    default_workers_test_config_ = """coordinator=false
discovery.uri=http://master:7070
http-server.http.port=7070
query.max-memory-per-node=512MB
query.max-memory=50GB\n"""

    default_node_properties_ = """catalog.config-dir=/etc/presto/catalog
node.data-dir=/var/lib/presto/data
node.environment=presto
node.launcher-log-file=/var/log/presto/launcher.log
node.server-log-file=/var/log/presto/server.log
plugin.dir=/usr/lib/presto/lib/plugin\n"""

    default_jvm_config_ = """-server
-Xmx16G
-XX:-UseBiasedLocking
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+UseGCOverheadLimit
-XX:+ExitOnOutOfMemoryError
-XX:ReservedCodeCacheSize=512M
-DHADOOP_USER_NAME=hive\n"""

    default_coordinator_config_ = """coordinator=true
discovery-server.enabled=true
discovery.uri=http://master:7070
http-server.http.port=7070
node-scheduler.include-coordinator=false
query.max-memory-per-node=8GB
query.max-memory=50GB\n"""

    default_coordinator_test_config_ = """coordinator=true
discovery-server.enabled=true
discovery.uri=http://master:7070
http-server.http.port=7070
node-scheduler.include-coordinator=false
query.max-memory-per-node=512MB
query.max-memory=50GB\n"""

    # The two strings below (down_node_connection_string and status_down_node_string) aggregate
    # all possible error messages one might encounter when trying to perform an action when a
    # node is not accessible. The variety in error messages comes from differences in the OS.
    down_node_connection_string = r'\nWarning: (\[%(host)s\] )?Name lookup failed for %(host)s'

    status_down_node_string = r'\tName lookup failed for %(host)s'

    len_down_node_error = 6

    def setUp(self):
        super(BaseProductTestCase, self).setUp()
        self.maxDiff = None
        self.cluster = None
        self.default_keywords = {}

    def tearDown(self):
        self.restore_stdout_stderr_keep_open()
        if self.cluster:
            self.cluster.tear_down()
        super(BaseProductTestCase, self).tearDown()

    def _apply_post_install_hooks(self, installers):
        for installer in installers:
            self.cluster.postinstall(installer)

    def _update_replacement_keywords(self, installers):
        for installer in installers:
            installer_instance = installer(self)
            self.default_keywords.update(installer_instance.get_keywords())

    def setup_cluster(self, bare_image_provider, cluster_type):
        installers = cluster_types[cluster_type]

        config_filename = ConfigurableCluster.check_for_cluster_config()

        if config_filename:
            self.cluster = ConfigurableCluster.start_bare_cluster(
                config_filename, self,
                StandalonePrestoInstaller.assert_installed)
            self.cluster.ensure_correct_execution_environment()
            BaseProductTestCase.run_installers(self.cluster, installers, self)
        else:
            self.cluster, bare_cluster = DockerCluster.start_cluster(
                bare_image_provider, cluster_type)
            self.cluster.ensure_correct_execution_environment()

            # If we've found images and started a non-bare cluster, the
            # containers have already had the installers applied to them.
            # We do need to get the test environment in sync with the
            # containers by calling the following two functions.
            #
            # We do this to save the cost of running the installers on the
            # docker containers every time we run a test. In practice,
            # that turns out to be a fairly expensive thing to do.
            if not bare_cluster:
                self._apply_post_install_hooks(installers)
                self._update_replacement_keywords(installers)
            else:
                raise RuntimeError("Docker images have not been created")

    # Do not call this method directory from tests or anywhere other than the BaseInstaller
    # implementation classes.
    @staticmethod
    def run_installers(cluster, installers, testcase):
        for installer in installers:
            dependencies = installer.get_dependencies()

            for dependency in dependencies:
                dependency.assert_installed(testcase)

            installer_instance = installer(testcase)
            installer_instance.install()

            testcase.default_keywords.update(installer_instance.get_keywords())
            cluster.postinstall(installer)

    def dump_and_cp_topology(self, topology, cluster=None):
        if not cluster:
            cluster = self.cluster
        cluster.write_content_to_host(
            json.dumps(topology),
            get_config_file_path(),
            cluster.master
        )

    def upload_topology(self, topology=None, cluster=None):
        if not cluster:
            cluster = self.cluster
        if not topology:
            topology = {"coordinator": "master",
                        "workers": ["slave1", "slave2", "slave3"]}
        self.dump_and_cp_topology(topology, cluster)

    @nottest
    def write_test_configs(self, cluster, extra_configs=None,
                           coordinator=None):
        if not coordinator:
            coordinator = self.cluster.internal_master
        config = 'http-server.http.port=7070\n' \
                 'query.max-memory=50GB\n' \
                 'query.max-memory-per-node=512MB\n' \
                 'discovery.uri=http://%s:7070' % coordinator
        if extra_configs:
            config += '\n' + extra_configs
        coordinator_config = '%s\n' \
                             'coordinator=true\n' \
                             'node-scheduler.include-coordinator=false\n' \
                             'discovery-server.enabled=true' % config
        workers_config = '%s\ncoordinator=false' % config
        cluster.write_content_to_host(
            coordinator_config,
            os.path.join(get_coordinator_directory(), 'config.properties'),
            cluster.master
        )
        cluster.write_content_to_host(
            workers_config,
            os.path.join(get_workers_directory(), 'config.properties'),
            cluster.master
        )

    def fetch_log_tail(self, lines=50):
        return self.cluster.exec_cmd_on_host(
            self.cluster.master,
            'tail -%d %s' % (lines, os.path.join(get_log_directory(), 'presto-admin.log')),
            raise_error=False)

    def run_prestoadmin(self, command, raise_error=True, cluster=None,
                        **kwargs):
        if not cluster:
            cluster = self.cluster
        command = self.replace_keywords(command, cluster=cluster, **kwargs)
        return cluster.exec_cmd_on_host(
            cluster.master,
            "{path} --user {user} {cmd}".format(path=get_presto_admin_path(), user=cluster.user, cmd=command),
            raise_error=raise_error,
            invoke_sudo=False
        )

    def run_script_from_prestoadmin_dir(self, script_contents, host='',
                                        raise_error=True, **kwargs):
        if not host:
            host = self.cluster.master

        script_contents = self.replace_keywords(script_contents,
                                                **kwargs)
        temp_script = os.path.join(get_install_directory(), 'tmp.sh')
        self.cluster.write_content_to_host(
            '#!/bin/bash\ncd %s\n%s' % (get_install_directory(), script_contents),
            temp_script, host)
        self.cluster.exec_cmd_on_host(
            host, 'chmod +x %s' % temp_script)
        return self.cluster.exec_cmd_on_host(
            host, temp_script, raise_error=raise_error)

    def run_prestoadmin_expect(self, command, expect_statements):
        temp_script = os.path.join(get_install_directory(), 'tmp.expect')
        script_content = '#!/usr/bin/expect\n' + \
                         'spawn %s %s\n%s' % \
                         (get_presto_admin_path(), command, expect_statements)

        self.cluster.write_content_to_host(script_content, temp_script,
                                           self.cluster.master)
        self.cluster.exec_cmd_on_host(
            self.cluster.master, 'chmod +x %s' % temp_script)
        return self.cluster.exec_cmd_on_host(
            self.cluster.master, temp_script)

    def assert_path_exists(self, host, file_path):
        self.cluster.exec_cmd_on_host(
            host, ' [ -e %s ] ' % file_path)

    def get_file_content(self, host, filepath):
        return self.cluster.exec_cmd_on_host(host, 'cat %s' % (filepath), invoke_sudo=True)

    def assert_config_perms(self, host, filepath):
        self.assert_file_perm_owner(
            host, filepath, '-rw-------', 'presto', 'presto')

    def assert_directory_perm_owner(self, host, filepath, permissions, owner, group):
        self.assertEqual(permissions[0], 'd', 'expected permissions should begin with a d')
        ls = self.cluster.exec_cmd_on_host(host, "ls -l -d %s" % filepath)
        self.assert_perm_owner(permissions, owner, group, ls)

    def assert_file_perm_owner(self, host, filepath, permissions, owner, group):
        ls = self.cluster.exec_cmd_on_host(host, "ls -l %s" % filepath)
        self.assert_perm_owner(permissions, owner, group, ls)

    def assert_perm_owner(self, permissions, owner, group, actual):
        fields = actual.split()
        self.assertEqual(fields[0], permissions)
        self.assertEqual(fields[2], owner)
        self.assertEqual(fields[3], group)

    def assert_file_content(self, host, filepath, expected):
        content = self.get_file_content(host, filepath)

        split_path = os.path.split(filepath)
        pa_file = None
        if (split_path[0] == '/etc/presto' and split_path[1] in ['config.properties', 'log.properties', 'jvm.config']):
            if host in self.cluster.slaves:
                config_dir = get_workers_directory()
            else:
                config_dir = get_coordinator_directory()

            pa_file = os.path.join(config_dir, split_path[1])

        self.assertLazyMessage(
            lambda: self.file_content_message(content, expected, pa_file),
            self.assertEqual,
            content,
            expected)

    def file_content_message(self, actual, expected, pa_file):
        msg = '\t===== vv ACTUAL FILE CONTENT vv =====\n' \
              '%s\n' \
              '\t=========== DID NOT EQUAL ===========\n' \
              '%s\n' \
              '\t==== ^^ EXPECTED FILE CONTENT ^^ ====\n' \
              '' % (actual, expected)
        if pa_file:
            try:
                # If the actual file content should have come from a file that
                # lives on the presto-admin host that we shove over to some
                # other host, display the content of the file as it is on the
                # presto-admin host. Presumably this will match the actual
                # file content that we display above.
                msg += '\t==== Content for presto-admin file %s ====\n' % (pa_file,)
                msg += self.get_file_content(self.cluster.master, pa_file)
                msg += '\n\t==========================================\n'
            except OSError as e:
                msg += e.message
        return msg

    def assert_file_content_regex(self, host, filepath, expected):
        config = self.get_file_content(host, filepath)
        self.assertRegexpMatches(config, expected)

    def assert_has_default_catalog(self, host):
        catalog_dir = constants.REMOTE_CATALOG_DIR
        self.assert_directory_perm_owner(host, catalog_dir, 'drwxr-xr-x', 'presto', 'presto')

        filepath = os.path.join(catalog_dir, 'tpch.properties')
        self.assert_config_perms(host, filepath)
        self.assert_file_content(host, filepath, 'connector.name=tpch')

    def assert_has_jmx_catalog(self, container):
        self.assert_file_content(container,
                                 '/etc/presto/catalog/jmx.properties',
                                 'connector.name=jmx')

    def assert_path_removed(self, container, directory):
        self.cluster.exec_cmd_on_host(
            container, ' [ ! -e %s ]' % directory)

    def assert_has_default_config(self, host):
        jvm_config_path = '/etc/presto/jvm.config'
        self.assert_config_perms(host, jvm_config_path)
        self.assert_file_content(
            host, jvm_config_path, self.default_jvm_config_)

        self.assert_node_config(host, self.default_node_properties_)

        config_properties_path = os.path.join(constants.REMOTE_CONF_DIR,
                                              'config.properties')

        self.assert_config_perms(host, config_properties_path)
        if host in self.cluster.slaves:
            self.assert_file_content(host, config_properties_path,
                                     self.default_workers_test_config_)

        else:
            self.assert_file_content(host, config_properties_path,
                                     self.default_coordinator_test_config_)

    def assert_node_config(self, host, expected, expected_node_id=None):
        node_properties_path = '/etc/presto/node.properties'
        self.assert_config_perms(host, node_properties_path)
        node_properties = self.cluster.exec_cmd_on_host(
            host, 'cat %s' % (node_properties_path,), invoke_sudo=True)
        split_properties = node_properties.split('\n', 1)
        if expected_node_id:
            self.assertEqual(expected_node_id, split_properties[0])
        else:
            self.assertRegexpMatches(split_properties[0], 'node.id=.*')
        actual = split_properties[1]
        if host in self.cluster.slaves:
            conf_dir = get_workers_directory()
        else:
            conf_dir = get_coordinator_directory()
        self.assertLazyMessage(
            lambda: self.file_content_message(actual, expected, os.path.join(conf_dir, 'node.properties')),
            self.assertEqual,
            actual,
            expected)

    def expected_stop(self, running=None, not_running=None):
        if running is None:
            running = self.cluster.all_internal_hosts()
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
                                    r'\[%s\] out: Not running' % host,
                                    r'\[%s\] out: Stopping presto' % host]

        return expected_output

    def assert_stopped(self, process_per_host):
        for host, pid in process_per_host:
            self.retry(lambda:
                       self.assertRaisesRegexp(OSError,
                                               'No such process',
                                               self.cluster.exec_cmd_on_host,
                                               host,
                                               'kill -0 %s' % pid),
                       retry_timeout=10,
                       retry_interval=2)

    @staticmethod
    def get_process_per_host(output_lines):
        process_per_host = []
        # We found some places where we were incorrectly passing a string
        # containing the output rather than an iterable collection of lines.
        # Since strings don't have an __iter__ attribute, we can catch this
        # error.
        if not hasattr(output_lines, '__iter__'):
            raise Exception('output_lines doesn\'t have an __iter__ ' +
                            'attribute. Did you pass an unsplit string?')
        for line in output_lines:
            match = re.search(r'\[(?P<host>.*?)\] out: Started as (?P<pid>.*)',
                              line)
            if match:
                process_per_host.append((match.group('host'),
                                         match.group('pid')))
        return process_per_host

    def assert_started(self, process_per_host):
        for host, pid in process_per_host:
            self.cluster.exec_cmd_on_host(host, 'kill -0 %s' % pid, invoke_sudo=True)
        return process_per_host

    def replace_keywords(self, text, cluster=None, **kwargs):
        if not cluster:
            cluster = self.cluster

        test_keywords = self.default_keywords.copy()
        test_keywords.update({
            'master': cluster.internal_master
        })
        if cluster.internal_slaves:
            test_keywords.update({
                'slave1': cluster.internal_slaves[0],
                'slave2': cluster.internal_slaves[1],
                'slave3': cluster.internal_slaves[2]
            })
        test_keywords.update(**kwargs)
        return text % test_keywords

    @staticmethod
    def escape_for_regex(expected):
        expected = expected.replace('[', '\[')
        expected = expected.replace(']', '\]')
        expected = expected.replace(')', '\)')
        expected = expected.replace('(', '\(')
        expected = expected.replace('+', '\+')
        return expected

    @staticmethod
    def retry(method_to_check, retry_timeout=RETRY_TIMEOUT,
              retry_interval=RETRY_INTERVAL):
        return Retrying(stop_max_delay=retry_timeout * 1000,
                        wait_fixed=retry_interval * 1000).call(method_to_check)

    def down_node_connection_error(self, host):
        hostname = self.cluster.get_down_hostname(host)
        return self.down_node_connection_string % {'host': hostname}

    def status_node_connection_error(self, host):
        hostname = self.cluster.get_down_hostname(host)
        return self.status_down_node_string % {'host': hostname}

    def create_presto_client(self, host=None):
        ips = self.cluster.get_ip_address_dict()
        config_path = os.path.join('~', LOCAL_CONF_DIR, COORDINATOR_DIR_NAME, CONFIG_PROPERTIES)
        config = self.cluster.exec_cmd_on_host(self.cluster.master, 'cat ' + config_path)
        user = 'root'
        if host is None:
            host = self.cluster.master
        return PrestoClient(ips[host], user, PrestoConfig.from_file(StringIO(config), config_path, host))


def docker_only(original_function):
    def test_inner(self, *args, **kwargs):
        if type(getattr(self, 'cluster')) is DockerCluster:
            original_function(self, *args, **kwargs)
        else:
            print 'Warning: Docker only test, passing with a noop'
    return test_inner


class PrestoError(Exception):
    pass
