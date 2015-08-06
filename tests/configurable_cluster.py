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

""" Cluster object used to control a cluster that can be configured with
a yaml file.

Test writers should use this module for all of their cluster related needs.
"""

import fnmatch
import os
import paramiko
from subprocess import check_call
import tempfile
import yaml

from prestoadmin import main_dir
from tests.product import user_password

CONFIG_FILE_GLOB = r'*.yaml'
DEFAULT_MOUNT_POINT = '/mnt/presto-admin'
DIST_DIR = os.path.join(main_dir, 'tmp/installer')


class ConfigurableCluster(object):
    """Start/stop/control/query a cluster defined by a configuration file.

    This class allows you to run the presto-admin product tests on a real
    cluster.

    The configuration file must specify one master and three slaves, and a
    user. That user must have sudo access on the cluster. If you want to
    teardown a cluster that already has presto installed, specify
    teardown_existing_cluster: true. An example config on vagrant:

    master: '172.16.1.10'
    slaves: ['172.16.1.11', '172.16.1.12', '172.16.1.13']
    user: root
    teardown_existing_cluster: true
    """
    def __init__(self, config_filename):
        with open(os.path.join(main_dir, config_filename)) as config_file:
            config = yaml.load(config_file)

        self.master = config['master']
        if type(self.master) is not str:
            raise Exception('Must have just one master with type string.')

        self.slaves = config['slaves']
        if len(self.slaves) is not 3 or type(self.slaves) is not list:
            raise Exception('Must specify three slaves in the config file.')

        self.internal_master = 'master'
        self.internal_slaves = ['slave1', 'slave2', 'slave3']
        self.user = config['user']
        self.password = user_password
        self.config = config
        self.mount_dir = DEFAULT_MOUNT_POINT

    @staticmethod
    def check_for_cluster_config():
        config_name = fnmatch.filter(os.listdir(main_dir), CONFIG_FILE_GLOB)
        if config_name:
            return config_name[0]
        else:
            return None

    def all_hosts(self):
        return self.slaves + [self.master]

    def all_internal_hosts(self, stopped_host=None):
        internal_hosts = self.internal_slaves + [self.internal_master]
        return internal_hosts

    def get_dist_dir(self, unique):
        if unique:
            return os.path.join(DIST_DIR, self.master)
        else:
            return DIST_DIR

    def tear_down(self):
        for host in self.all_hosts():
            script = """
            service presto stop
            rpm -e presto-server-rpm
            rm -rf /opt/prestoadmin
            rm -rf /etc/opt/prestoadmin
            rm -rf /tmp/presto-debug
            rm -rf /etc/presto/
            rm -rf %s
            """ % self.mount_dir
            self.run_script_on_host(script, host)

    def stop_host(self, host_name):
        if host_name not in self.all_hosts():
            raise Exception('Must specify external hostname to stop_host')

        # Change the topology to something that doesn't exist
        ips = self.get_ip_address_dict()
        down_hostname = self.get_down_hostname(host_name)
        self.exec_cmd_on_host(
            self.master,
            'sed -i s/%s/%s/g /etc/opt/prestoadmin/config.json' %
            host_name, down_hostname
        )
        self.exec_cmd_on_host(
            self.master,
            'sed -i s/%s/%s/g /etc/opt/prestoadmin/config.json'
            % (ips[host_name], down_hostname)
        )
        index = self.all_hosts().index(host_name)
        self.exec_cmd_on_host(
            self.master,
            'sed -i s/%s/%s/g /etc/opt/prestoadmin/config.json' %
            (self.all_internal_hosts()[index], down_hostname)
        )

        if index >= len(self.internal_slaves):
            self.internal_master = down_hostname
        else:
            self.internal_slaves[index] = down_hostname

    def get_down_hostname(self, host_name):
        return '1.0.0.0'

    def exec_cmd_on_host(self, host, cmd, raise_error=True, tty=False):
        # We need to execute the commands on the external, not internal, host.
        if host not in self.all_hosts():
            index = self.all_internal_hosts().index(host)
            host = self.all_hosts()[index]
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, username=self.user, password=self.password,
                    timeout=180)
        stdin, stdout, stderr = ssh.exec_command(cmd, get_pty=True)
        stdin.close()
        output = ''.join(stdout.readlines()).replace('\r', '')\
            .encode('ascii', 'ignore')
        exit_status = stdout.channel.recv_exit_status()
        ssh.close()
        if exit_status and raise_error:
            raise OSError(exit_status, output)
        return output

    @staticmethod
    def start_presto_cluster(config_filename, install_func, assert_installed):
        presto_cluster = ConfigurableCluster.start_base_cluster(
            config_filename, assert_installed)
        install_func(presto_cluster)
        return presto_cluster

    @staticmethod
    def start_base_cluster(config_filename, assert_installed):
        centos_cluster = ConfigurableCluster(config_filename)
        if 'teardown_existing_cluster' in centos_cluster.config \
                and centos_cluster.config['teardown_existing_cluster']:
            centos_cluster.tear_down()
        elif centos_cluster._presto_is_installed(assert_installed):
            raise Exception('Cluster already has Presto installed, '
                            'either uninstall Presto or specify '
                            '\'teardown_existing_cluster: true\' in the '
                            'cluster.yaml file.')
        return centos_cluster

    def run_script_on_host(self, script_contents, host):
        temp_script = '/tmp/tmp.sh'
        self.write_content_to_host('#!/bin/bash\n%s' % script_contents,
                                   temp_script, host)
        self.exec_cmd_on_host(host, 'chmod +x %s' % temp_script)
        return self.exec_cmd_on_host(host, temp_script, tty=True)

    def write_content_to_host(self, content, remote_path, host):
        with tempfile.NamedTemporaryFile('w', dir='/tmp', delete=False) \
                as temp_config_file:
            temp_config_file.write(content)
            temp_config_file.close()
            self.copy_to_host(temp_config_file.name, host,
                              dest_path=remote_path)
            check_call(['rm', temp_config_file.name])

    def copy_to_host(self, source_path, host, dest_path=None):
        if not dest_path:
            dest_path = os.path.join(self.mount_dir,
                                     os.path.basename(source_path))
        self.exec_cmd_on_host(host, 'mkdir -p ' + os.path.dirname(dest_path))

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, username=self.user, password=self.password,
                    timeout=180)
        sftp = ssh.open_sftp()
        sftp.put(source_path, dest_path)
        sftp.close()
        ssh.close()

    def get_ip_address_dict(self):
        hosts_file = self.exec_cmd_on_host(self.master, 'cat /etc/hosts')\
            .splitlines()
        ip_addresses = {}
        for host in self.all_hosts():
            ip_addresses[host] = self._get_ip_from_hosts_file(hosts_file, host)

        for internal_host in self.all_internal_hosts():
            ip_addresses[internal_host] = self._get_ip_from_hosts_file(
                hosts_file, internal_host)
        return ip_addresses

    def _get_ip_from_hosts_file(self, hosts_file, host):
        for line in hosts_file:
            if host in line:
                return line.split(' ')[0]
        return None

    def _presto_is_installed(self, assert_installed):
        for host in self.all_hosts():
            try:
                assert_installed(host, cluster=self)
            except AssertionError:
                return True
        return False
