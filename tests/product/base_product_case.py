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
from time import sleep

from nose.tools import nottest

from prestoadmin.util import constants
from tests.base_test_case import BaseTestCase
from tests.configurable_cluster import ConfigurableCluster
from tests.docker_cluster import DockerCluster, DockerClusterException

from tests.product.mode_installers import StandaloneModeInstaller, \
    YarnSliderModeInstaller
from tests.product.prestoadmin_installer import PrestoadminInstaller
from tests.product.standalone.presto_installer import StandalonePrestoInstaller
from tests.product.topology_installer import TopologyInstaller
from tests.product.yarn_slider.slider_installer import SliderInstaller

PRESTO_VERSION = r'.+'
RETRY_TIMEOUT = 120
RETRY_INTERVAL = 5


class BaseProductTestCase(BaseTestCase):
    STANDALONE_BARE_CLUSTER = 'bare'
    BARE_CLUSTER = 'bare'
    PA_ONLY_CLUSTER = 'pa_only_standalone'
    STANDALONE_PRESTO_CLUSTER = 'presto'

    PA_ONLY_YS_CLUSTER = 'pa_only_ys'
    PA_SLIDER_CLUSTER = 'pa_slider'

    _cluster_types = {
        BARE_CLUSTER: [],
        PA_ONLY_CLUSTER: [PrestoadminInstaller,
                          StandaloneModeInstaller],
        STANDALONE_PRESTO_CLUSTER: [PrestoadminInstaller,
                                    StandaloneModeInstaller,
                                    TopologyInstaller,
                                    StandalonePrestoInstaller],
        PA_ONLY_YS_CLUSTER: [PrestoadminInstaller,
                             YarnSliderModeInstaller],
        PA_SLIDER_CLUSTER: [PrestoadminInstaller,
                            YarnSliderModeInstaller,
                            SliderInstaller]
    }

    default_workers_config_ = """coordinator=false
discovery.uri=http://master:8080
http-server.http.port=8080
query.max-memory-per-node=1GB
query.max-memory=50GB\n"""

    default_workers_test_config_ = """coordinator=false
discovery.uri=http://master:8080
http-server.http.port=8080
query.max-memory-per-node=512MB
query.max-memory=50GB\n"""

    default_node_properties_ = """node.data-dir=/var/lib/presto/data
node.environment=presto
plugin.config-dir=/etc/presto/catalog
plugin.dir=/usr/lib/presto/lib/plugin\n"""

    default_jvm_config_ = """-server
-Xmx2G
-XX:-UseBiasedLocking
-XX:+UseG1GC
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+UseGCOverheadLimit
-XX:OnOutOfMemoryError=kill -9 %p
-DHADOOP_USER_NAME=hive\n"""

    default_coordinator_config_ = """coordinator=true
discovery-server.enabled=true
discovery.uri=http://master:8080
http-server.http.port=8080
node-scheduler.include-coordinator=false
query.max-memory-per-node=1GB
query.max-memory=50GB\n"""

    default_coordinator_test_config_ = """coordinator=true
discovery-server.enabled=true
discovery.uri=http://master:8080
http-server.http.port=8080
node-scheduler.include-coordinator=false
query.max-memory-per-node=512MB
query.max-memory=50GB\n"""

    down_node_connection_string = r'(\nWarning: (\[%(host)s\] )?Low level socket ' \
                                  r'error connecting to host %(host)s on ' \
                                  r'port 22: No route to host ' \
                                  r'\(tried 1 time\)\n\nUnderlying ' \
                                  r'exception:\n    No route to host\n' \
                                  r'|\nWarning: (\[%(host)s] )?Timed out trying ' \
                                  r'to connect to %(host)s \(tried 1 ' \
                                  r'time\)\n\nUnderlying exception:' \
                                  r'\n    timed out\n)'

    status_down_node_string = r'(\tLow level socket error connecting to host ' \
                              r'%(host)s on port 22: No route to host \(tried ' \
                              r'1 time\)|\tTimed out trying to connect to ' \
                              r'%(host)s \(tried 1 time\))'

    len_down_node_error = 6

    def setUp(self):
        super(BaseProductTestCase, self).setUp()
        self.maxDiff = None
        self.cluster = None
        self.default_keywords = {}

    def _run_installers(self, installers):
        cluster = self.cluster
        for installer in installers:
            dependencies = installer.get_dependencies()

            for dependency in dependencies:
                dependency.assert_installed(self)

            installer_instance = installer(self)
            installer_instance.install()

            self.default_keywords.update(installer_instance.get_keywords())
            cluster.postinstall(installer)

    def _apply_post_install_hooks(self, installers):
        for installer in installers:
            self.cluster.postinstall(installer)

    def _update_replacement_keywords(self, installers):
        for installer in installers:
            installer_instance = installer(self)
            self.default_keywords.update(installer_instance.get_keywords())

    def setup_cluster(self, bare_image_provider, cluster_type):
        try:
            installers = self._cluster_types[cluster_type]
        except KeyError:
            self.fail(
                '%s is not a valid cluster type. Valid cluster types are %s' %
                (cluster_type, ', '.join(self._cluster_types.keys())))

        config_filename = ConfigurableCluster.check_for_cluster_config()

        if config_filename:
            self.cluster = ConfigurableCluster.start_bare_cluster(
                config_filename, self,
                StandalonePrestoInstaller.assert_installed)
        else:
            try:
                self.cluster = DockerCluster.start_existing_images(
                    bare_image_provider, cluster_type)
                if self.cluster:
                    self._apply_post_install_hooks(installers)
                    self._update_replacement_keywords(installers)
                    return
                self.cluster = DockerCluster.start_bare_cluster(
                    bare_image_provider)
            except DockerClusterException as e:
                self.fail(e.msg)

        self._run_installers(installers)

        if isinstance(self.cluster, DockerCluster):
            self.cluster.commit_images(bare_image_provider, cluster_type)

    def tearDown(self):
        self.restore_stdout_stderr_keep_open()
        if self.cluster:
            self.cluster.tear_down()
        super(BaseProductTestCase, self).tearDown()

    def dump_and_cp_topology(self, topology, cluster=None):
        if not cluster:
            cluster = self.cluster
        cluster.write_content_to_host(
            json.dumps(topology),
            '/etc/opt/prestoadmin/config.json',
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
        config = 'http-server.http.port=8080\n' \
                 'query.max-memory=50GB\n' \
                 'query.max-memory-per-node=512MB\n' \
                 'discovery.uri=http://%s:8080' % coordinator
        if extra_configs:
            config += '\n' + extra_configs
        coordinator_config = '%s\n' \
                             'coordinator=true\n' \
                             'node-scheduler.include-coordinator=false\n' \
                             'discovery-server.enabled=true' % config
        workers_config = '%s\ncoordinator=false' % config
        cluster.write_content_to_host(
            coordinator_config,
            os.path.join(constants.COORDINATOR_DIR, 'config.properties'),
            cluster.master
        )
        cluster.write_content_to_host(
            workers_config,
            os.path.join(constants.WORKERS_DIR, 'config.properties'),
            cluster.master
        )

    def run_prestoadmin(self, command, raise_error=True, cluster=None,
                        **kwargs):
        if not cluster:
            cluster = self.cluster
        command = self.replace_keywords(command, cluster=cluster, **kwargs)
        return cluster.exec_cmd_on_host(
            cluster.master,
            "/opt/prestoadmin/presto-admin %s" % command,
            raise_error=raise_error
        )

    def run_script_from_prestoadmin_dir(self, script_contents, host='',
                                        raise_error=True, **kwargs):
        if not host:
            host = self.cluster.master

        script_contents = self.replace_keywords(script_contents,
                                                **kwargs)
        temp_script = '/opt/prestoadmin/tmp.sh'
        self.cluster.write_content_to_host(
            '#!/bin/bash\ncd /opt/prestoadmin\n%s' % script_contents,
            temp_script, host)
        self.cluster.exec_cmd_on_host(
            host, 'chmod +x %s' % temp_script)
        return self.cluster.exec_cmd_on_host(
            host, temp_script, raise_error=raise_error)

    def run_prestoadmin_expect(self, command, expect_statements):
        temp_script = '/opt/prestoadmin/tmp.expect'
        script_content = '#!/usr/bin/expect\n' + \
                         'spawn /opt/prestoadmin/presto-admin %s\n%s' % \
                         (command, expect_statements)

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
        return self.cluster.exec_cmd_on_host(host, 'cat %s' % (filepath))

    def assert_file_content(self, host, filepath, expected):
        config = self.get_file_content(host, filepath)
        self.assertEqual(config, expected)

    def assert_file_content_regex(self, host, filepath, expected):
        config = self.get_file_content(host, filepath)
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
        self.cluster.exec_cmd_on_host(
            container, ' [ ! -e %s ]' % directory)

    def assert_has_default_config(self, container):
        self.assert_file_content(container,
                                 '/etc/presto/jvm.config',
                                 self.default_jvm_config_)

        self.assert_node_config(container, self.default_node_properties_)

        if container in self.cluster.slaves:
            self.assert_file_content(container,
                                     '/etc/presto/config.properties',
                                     self.default_workers_test_config_)

        else:
            self.assert_file_content(container,
                                     '/etc/presto/config.properties',
                                     self.default_coordinator_test_config_)

    def assert_node_config(self, container, expected):
        node_properties = self.cluster.exec_cmd_on_host(
            container, 'cat /etc/presto/node.properties')
        split_properties = node_properties.split('\n', 1)
        self.assertRegexpMatches(split_properties[0], 'node.id=.*')
        self.assertEqual(expected, split_properties[1])

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
                                    r'\[%s\] out: Not runnning' % host,
                                    r'\[%s\] out: Stopping presto' % host]

        return expected_output

    def assert_stopped(self, process_per_host):
        for host, pid in process_per_host:
            self.assertRaisesRegexp(OSError,
                                    'No such process',
                                    self.cluster.exec_cmd_on_host,
                                    host, 'kill -0 %s' % pid)

    def get_process_per_host(self, output_lines):
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
            self.cluster.exec_cmd_on_host(host, 'kill -0 %s' % pid)
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

    def escape_for_regex(self, expected):
        expected = expected.replace('[', '\[')
        expected = expected.replace(']', '\]')
        expected = expected.replace(')', '\)')
        expected = expected.replace('(', '\(')
        expected = expected.replace('+', '\+')
        return expected

    def retry(self, method_to_check):
        time_spent_waiting = 0
        while time_spent_waiting <= RETRY_TIMEOUT:
            try:
                result = method_to_check()
                # No exception thrown, success
                return result
            except (AssertionError, PrestoError, OSError):
                pass
            sleep(RETRY_INTERVAL)
            time_spent_waiting += RETRY_INTERVAL
        return method_to_check()

    def down_node_connection_error(self, host):
        hostname = self.cluster.get_down_hostname(host)
        return self.down_node_connection_string % {'host': hostname}

    def status_node_connection_error(self, host):
        hostname = self.cluster.get_down_hostname(host)
        return self.status_down_node_string % {'host': hostname}


def docker_only(original_function):
    def test_inner(self, *args, **kwargs):
        if type(getattr(self, 'cluster')) is DockerCluster:
            original_function(self, *args, **kwargs)
        else:
            print 'Warning: Docker only test, passing with a noop'
    return test_inner


class PrestoError(Exception):
    pass
