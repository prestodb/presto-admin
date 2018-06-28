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
Product tests for presto-admin status commands
"""

from nose.plugins.attrib import attr

from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase, \
    PRESTO_VERSION, PrestoError
from tests.product.cluster_types import STANDALONE_PA_CLUSTER, STANDALONE_PRESTO_CLUSTER
from tests.product.standalone.presto_installer import StandalonePrestoInstaller


class TestStatus(BaseProductTestCase):

    def setUp(self):
        super(TestStatus, self).setUp()
        self.installer = StandalonePrestoInstaller(self)

    def test_status_uninstalled(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PA_CLUSTER)
        self.upload_topology()
        status_output = self._server_status_with_retries()
        self.check_status(status_output, self.not_installed_status())

    def test_status_not_started(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PRESTO_CLUSTER)
        status_output = self._server_status_with_retries()
        self.check_status(status_output, self.not_started_status())

    @attr('smoketest')
    def test_status_happy_path(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PRESTO_CLUSTER)
        self.run_prestoadmin('server start')
        status_output = self._server_status_with_retries(check_catalogs=True)
        self.check_status(status_output, self.base_status())

    def test_status_only_coordinator(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PRESTO_CLUSTER)

        self.run_prestoadmin('server start -H master')
        # don't run with retries because it won't be able to query the
        # coordinator because the coordinator is set to not be a worker
        status_output = self.run_prestoadmin('server status')
        self.check_status(
            status_output,
            self.single_node_up_status(self.cluster.internal_master)
        )

    def test_status_only_worker(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PRESTO_CLUSTER)

        self.run_prestoadmin('server start -H slave1')
        status_output = self._server_status_with_retries()
        self.check_status(
            status_output,
            self.single_node_up_status(self.cluster.internal_slaves[0])
        )

        # Check that the slave sees that it's stopped, even though the
        # discovery server is not up.
        self.run_prestoadmin('server stop')
        status_output = self._server_status_with_retries()
        self.check_status(status_output, self.not_started_status())

    def test_connection_to_coordinator_lost(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PA_CLUSTER)
        topology = {"coordinator": "slave1", "workers":
                    ["master", "slave2", "slave3"]}
        self.upload_topology(topology=topology)
        self.installer.install(coordinator='slave1')
        self.run_prestoadmin('server start')
        self.cluster.stop_host(
            self.cluster.slaves[0])
        topology = {"coordinator": self.cluster.get_down_hostname("slave1"),
                    "workers": ["master", "slave2", "slave3"]}
        status_output = self._server_status_with_retries()
        statuses = self.node_not_available_status(
            topology, self.cluster.internal_slaves[0],
            coordinator_down=True)
        self.check_status(status_output, statuses)

    def test_connection_to_worker_lost(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PA_CLUSTER)
        topology = {"coordinator": "slave1", "workers":
                    ["master", "slave2", "slave3"]}
        self.upload_topology(topology=topology)
        self.installer.install(coordinator='slave1')
        self.run_prestoadmin('server start')
        self.cluster.stop_host(
            self.cluster.slaves[1])
        topology = {"coordinator": "slave1", "workers":
                    ["master", self.cluster.get_down_hostname("slave2"),
                     "slave3"]}
        status_output = self._server_status_with_retries(check_catalogs=True)
        statuses = self.node_not_available_status(
            topology, self.cluster.internal_slaves[1])
        self.check_status(status_output, statuses)

    def test_status_non_root_user(self):
        self.setup_cluster(NoHadoopBareImageProvider(), STANDALONE_PRESTO_CLUSTER)
        self.upload_topology(
            {"coordinator": "master",
             "workers": ["slave1", "slave2", "slave3"],
             "username": "app-admin"}
        )
        self.run_prestoadmin('server start -p password')
        status_output = self._server_status_with_retries(check_catalogs=True, extra_arguments=' -p password')
        self.check_status(status_output, self.base_status())

    def base_status(self, topology=None):
        ips = self.cluster.get_ip_address_dict()
        if not topology:
            topology = {
                'coordinator': self.cluster.internal_master, 'workers':
                [self.cluster.internal_slaves[0],
                 self.cluster.internal_slaves[1],
                 self.cluster.internal_slaves[2]]
            }
        statuses = []
        hosts_in_status = [topology['coordinator']] + topology['workers'][:]
        for host in hosts_in_status:
            role = 'coordinator' if host is topology['coordinator']\
                else 'worker'
            status = {'host': host, 'role': role, 'ip': ips[host],
                      'is_running': 'Running'}
            statuses += [status]
        return statuses

    def not_started_status(self):
        statuses = self.base_status()
        for status in statuses:
            status['ip'] = 'Unknown'
            status['is_running'] = 'Not Running'
            status['error_message'] = '\tNo information available: ' \
                                      'unable to query coordinator'
        return statuses

    def not_installed_status(self):
        statuses = self.base_status()
        for status in statuses:
            status['ip'] = 'Unknown'
            status['is_running'] = 'Not Running'
            status['error_message'] = '\tPresto is not installed.'
        return statuses

    def single_node_up_status(self, node):
        statuses = self.not_started_status()
        for status in statuses:
            if status['host'] is node:
                status['is_running'] = 'Running'
        return statuses

    def node_not_available_status(self, topology, node,
                                  coordinator_down=False):
        statuses = self.base_status(topology)
        for status in statuses:
            if status['host'] == node:
                status['is_running'] = 'Not Running'
                status['error_message'] = \
                    self.status_node_connection_error(node)
                status['ip'] = 'Unknown'
                status['host'] = self.cluster.get_down_hostname(node)
            elif coordinator_down:
                status['error_message'] = '\tNo information available: ' \
                                          'unable to query coordinator'
                status['ip'] = 'Unknown'

        return statuses

    def status_fail_msg(self, actual_output, expected_regexp):
        log_tail = self.fetch_log_tail(lines=100)

        return (
            '=== ACTUAL OUTPUT ===\n%s\n=== DID NOT MATCH REGEXP ===\n%s\n'
            '=== LOG FOR DEBUGGING ===\n%s=== END OF LOG ===' % (
                actual_output, expected_regexp, log_tail))

    def check_status(self, cmd_output, statuses, port=7070):
        expected_output = []
        for status in statuses:
            expected_output += \
                ['Server Status:',
                 '\t%s\(IP: .+, Roles: %s\): %s' %
                 (status['host'], status['role'], status['is_running'])]
            if 'error_message' in status and status['error_message']:
                expected_output += [status['error_message']]
            elif status['is_running'] is 'Running':
                expected_output += \
                    ['\tNode URI\(http\): http://.+:%s' % str(port),
                     '\tPresto Version: ' + PRESTO_VERSION,
                     '\tNode status:    active',
                     '\tCatalogs:     system, tpch']

        expected_regex = '\n'.join(expected_output)
        # The status command is written such that there are a couple ways that
        # the presto client can fail that result in partial output from the
        # command, but errors in the logs. If we fail to match, we include the
        # log information in the assertion message to make determining exactly
        # what failed easier. Grab the logs lazily so that we don't incur the
        # cost of getting them when they aren't needed. The status tests are
        # slow enough already.
        self.assertLazyMessage(
            lambda: self.status_fail_msg(cmd_output, expected_regex),
            self.assertRegexpMatches, cmd_output, expected_regex)

    def _server_status_with_retries(self, check_catalogs=False, extra_arguments=''):
        try:
            return self.retry(lambda: self._get_status_until_coordinator_updated(
                check_catalogs, extra_arguments=extra_arguments), 720, 0)
        except PrestoError as e:
            self.assertLazyMessage(
                lambda: self.status_fail_msg(e.message, "Ran out of time retrying status"),
                self.fail,
                "PrestoError: %s" % e.message)

    def _get_status_until_coordinator_updated(self, check_catalogs=False, extra_arguments=''):
        status_output = self.run_prestoadmin('server status' + extra_arguments)
        if 'the coordinator has not yet discovered this node' in status_output:
            raise PrestoError('Coordinator has not discovered all nodes yet: '
                              '%s' % status_output)
        if 'Roles: coordinator): Running\n\tNo information available: ' \
           'unable to query coordinator' in status_output:
            raise PrestoError('Coordinator not started up properly yet.'
                              '\nOutput: %s' % status_output)
        if check_catalogs and 'Catalogs:' not in status_output:
            raise PrestoError('Catalogs not loaded yet: %s' % status_output)
        return status_output
