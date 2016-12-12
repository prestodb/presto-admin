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
import tempfile
import uuid
from subprocess import check_call

import paramiko
import yaml
from prestoadmin import main_dir
from tests.base_cluster import BaseCluster
from tests.product.config_dir_utils import get_config_file_path, get_install_directory, get_config_directory

CONFIG_FILE_GLOB = r'*.yaml'
DIST_DIR = os.path.join(main_dir, 'tmp/installer')


class ConfigurableCluster(BaseCluster):
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
    key_path: /path/to/cluster-key.pem
    mount_point: /home/ec2-user/presto-admin
    rpm_cache_dir: /home/ec2-user/presto-rpm-cache
    """

    def __init__(self, config_filename):
        with open(os.path.join(main_dir, config_filename)) as config_file:
            config = yaml.load(config_file)

        self._master = config['master']
        if type(self.master) is not str:
            raise Exception('Must have just one master with type string.')

        self.slaves = config['slaves']
        if len(self.slaves) is not 3 or type(self.slaves) is not list:
            raise Exception('Must specify three slaves in the config file.')

        self.internal_master = 'master'
        self.internal_slaves = ['slave1', 'slave2', 'slave3']
        self._user = config['user']

        self.key_path = config['key_path']
        if not os.path.exists(self.key_path):
            raise Exception('Key path specified {path} does not exist.'.format(
                path=self.key_path))

        self.config = config
        self._mount_dir = config['mount_point']
        self._rpm_cache_dir = config['rpm_cache_dir']

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
            # Remove the rm -rf /var/log/presto when the following issue
            # is resolved https://github.com/prestodb/presto-admin/issues/226
            script = """
            sudo service presto stop
            sudo rpm -e presto-server-rpm
            rm -rf {install_dir}
            rm -rf ~/prestoadmin*.tar.gz
            rm -rf {config_dir}
            sudo rm -rf /etc/presto/
            sudo rm -rf /usr/lib/presto/
            sudo rm -rf /tmp/presto-debug
            sudo rm -rf /tmp/presto-debug-remote
            sudo rm -rf /var/log/presto
            rm -rf {mount_dir}
            """.format(install_dir=get_install_directory(),
                       config_dir=get_config_directory(),
                       mount_dir=self.mount_dir)
            self.run_script_on_host(script, host)

    def stop_host(self, host_name):
        if host_name not in self.all_hosts():
            raise Exception('Must specify external hostname to stop_host')

        # Change the topology to something that doesn't exist
        ips = self.get_ip_address_dict()
        down_hostname = self.get_down_hostname(host_name)
        self.exec_cmd_on_host(
            self.master,
            'sed -i s/%s/%s/g %s' % (host_name, down_hostname, get_config_file_path())
        )
        self.exec_cmd_on_host(
            self.master,
            'sed -i s/%s/%s/g %s' % (ips[host_name], down_hostname, get_config_file_path())
        )
        index = self.all_hosts().index(host_name)
        self.exec_cmd_on_host(
            self.master,
            'sed -i s/%s/%s/g %s' % (self.all_internal_hosts()[index], down_hostname, get_config_file_path())
        )

        if index >= len(self.internal_slaves):
            self.internal_master = down_hostname
        else:
            self.internal_slaves[index] = down_hostname

    def get_down_hostname(self, host_name):
        return '1.0.0.0'

    def exec_cmd_on_host(self, host, cmd, user=None, raise_error=True,
                         tty=False, invoke_sudo=False):
        # If the corresponding variable is set, invoke command with sudo since EMR's login
        # user is ec2-user. If sudo is already present in the command then no error will occur
        # as arbitrary nesting of sudo is allowed.
        if invoke_sudo:
            cmd = 'sudo ' + cmd

        if user is None:
            user = self.user
        # We need to execute the commands on the external, not internal, host.
        if host not in self.all_hosts():
            index = self.all_internal_hosts().index(host)
            host = self.all_hosts()[index]
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, username=user, key_filename=self.key_path,
                    timeout=180)
        stdin, stdout, stderr = ssh.exec_command(cmd, get_pty=True)
        stdin.close()
        output = ''.join(stdout.readlines()).replace('\r', '') \
            .encode('ascii', 'ignore')
        exit_status = stdout.channel.recv_exit_status()
        ssh.close()
        if exit_status and raise_error:
            raise OSError(exit_status, output)
        return output

    @staticmethod
    def start_bare_cluster(config_filename, testcase, assert_installed):
        cluster = ConfigurableCluster(config_filename)
        if 'teardown_existing_cluster' in cluster.config \
                and cluster.config['teardown_existing_cluster']:
            cluster.tear_down()
        elif cluster._presto_is_installed(testcase, assert_installed):
            raise Exception('Cluster already has Presto installed, '
                            'either uninstall Presto or specify '
                            '\'teardown_existing_cluster: true\' in the '
                            'cluster.yaml file.')
        return cluster

    def run_script_on_host(self, script_contents, host, tty=True):
        temp_script = '~/tmp.sh'
        self.write_content_to_host('#!/bin/bash\n%s' % script_contents,
                                   temp_script, host)
        self.exec_cmd_on_host(host, 'chmod +x %s' % temp_script)
        return self.exec_cmd_on_host(host, temp_script, tty=tty)

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
        self.exec_cmd_on_host(host, 'mkdir -p {dir}'.format(dir=os.path.dirname(dest_path)))

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, username=self.user, key_filename=self.key_path,
                    timeout=180)

        # Upload to dummy location because paramiko doesn't allow SFTP using
        # sudo when logged in as a non root user. Due to this limitation, Fabric
        # uses the same methodology to upload files.
        dummy_path = '/tmp/{random_dir}/{dest_dir}'.format(
            random_dir=str(uuid.uuid1()), dest_dir=os.path.basename(dest_path))
        self.exec_cmd_on_host(host, 'mkdir -p {dir}'.format(dir=os.path.dirname(dummy_path)))
        sftp = ssh.open_sftp()
        sftp.put(source_path, dummy_path)
        sftp.close()

        # Move to final location using sudo
        self.exec_cmd_on_host(host, 'mv {source} {dest}'.format(source=dummy_path, dest=dest_path), invoke_sudo=True)

        # Remove dummy path directory
        self.exec_cmd_on_host(host, 'rm -rf {dir}'.format(dir=os.path.dirname(dummy_path)))

        ssh.close()

    # Since ConfigurableCluster is configured using external IPs, those act as
    # hosts and so the dict returned contains an identity mapping from external IPs
    # to external IPs in addition to internal host to internal IP mappings
    def get_ip_address_dict(self):
        ip_addresses = {}
        for ip in self.all_hosts():
            ip_addresses[ip] = ip

        hosts_file = self.exec_cmd_on_host(self.master, 'cat /etc/hosts').splitlines()
        for internal_host in self.all_internal_hosts():
            ip_addresses[internal_host] = self._get_ip_from_hosts_file(
                hosts_file, internal_host)
        return ip_addresses

    @staticmethod
    def _get_ip_from_hosts_file(hosts_file, host):
        for line in hosts_file:
            if host in line:
                return line.split(' ')[0]
        return None

    def _presto_is_installed(self, testcase, assert_installed):
        for host in self.all_hosts():
            try:
                assert_installed(testcase, host, cluster=self)
            except AssertionError:
                return False
        return True

    def postinstall(self, installer):
        pass

    @property
    def rpm_cache_dir(self):
        return self._rpm_cache_dir

    @property
    def mount_dir(self):
        return self._mount_dir

    @property
    def user(self):
        return self._user

    @property
    def master(self):
        return self._master
