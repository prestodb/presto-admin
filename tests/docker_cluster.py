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

"""Docker related functions, constants and objects needed by product tests.

Test writers should use this module for all of their docker related needs
and not directly call into the docker-py API.
"""

import subprocess
import sys
import shutil
import os
import errno
import uuid
from time import sleep

from docker import Client
from docker.errors import APIError
from docker.utils.utils import kwargs_from_env

from prestoadmin import main_dir
from tests.base_cluster import BaseCluster
from tests.product.constants import \
    DEFAULT_DOCKER_MOUNT_POINT, DEFAULT_LOCAL_MOUNT_POINT, \
    BASE_TD_IMAGE_NAME

DIST_DIR = os.path.join(main_dir, 'dist')


class DockerCluster(BaseCluster):
    IMAGE_NAME_BASE = os.path.join('teradatalabs', 'pa_test')
    BARE_CLUSTER_TYPE = 'bare'

    """Start/stop/control/query arbitrary clusters of docker containers.

    This class is aimed at product test writers to create docker containers
    for testing purposes.

    """
    def __init__(self, master_host, slave_hosts,
                 local_mount_dir, docker_mount_dir):
        # see PyDoc for all_internal_hosts() for an explanation on the
        # difference between an internal and regular host
        self.internal_master = master_host
        self.internal_slaves = slave_hosts
        self.master = master_host + '-' + str(uuid.uuid4())
        self.slaves = [slave + '-' + str(uuid.uuid4())
                       for slave in slave_hosts]
        # the root path for all local mount points; to get a particular
        # container mount point call get_local_mount_dir()
        self.local_mount_dir = local_mount_dir
        self.mount_dir = docker_mount_dir

        kwargs = kwargs_from_env()
        if 'tls' in kwargs:
            kwargs['tls'].assert_hostname = False
        kwargs['timeout'] = 300
        self.client = Client(**kwargs)

        self._DOCKER_START_TIMEOUT = 30
        DockerCluster.__check_if_docker_exists()

    def all_hosts(self):
        return self.slaves + [self.master]

    def get_master(self):
        return self.master

    def all_internal_hosts(self):
        return [host.split('-')[0] for host in self.all_hosts()]

    def get_local_mount_dir(self, host):
        return os.path.join(self.local_mount_dir,
                            self.__get_unique_host(host))

    def get_dist_dir(self, unique):
        if unique:
            return os.path.join(DIST_DIR, self.master)
        else:
            return DIST_DIR

    def __get_unique_host(self, host):
        matches = [unique_host for unique_host in self.all_hosts()
                   if unique_host.startswith(host)]
        if matches:
            return matches[0]
        elif host in self.all_hosts():
            return host
        else:
            raise DockerClusterException(
                'Specified host: {0} does not exist.'.format(host))

    @staticmethod
    def __check_if_docker_exists():
        try:
            subprocess.call(['docker', '--version'])
        except OSError:
            sys.exit('Docker is not installed. Try installing it with '
                     'presto-admin/bin/install-docker.sh.')

    def fetch_image_if_not_present(self, image, tag=None):
        if not tag and not self.client.images(image):
            self._execute_and_wait(self.client.pull, image)
        elif tag and not self._is_image_present_locally(image, tag):
            self._execute_and_wait(self.client.pull, image, tag)

    def _is_image_present_locally(self, image_name, tag):
        image_name_and_tag = image_name + ':' + tag
        images = self.client.images(image_name)
        if images:
            for image in images:
                if image_name_and_tag in image['RepoTags']:
                    return True
        return False

    def start_containers(self, master_image, slave_image=None,
                         cmd=None, **kwargs):
        self._create_host_mount_dirs()

        self._create_and_start_containers(master_image, slave_image,
                                          cmd, **kwargs)
        self._ensure_docker_containers_started(master_image)
        sleep(3)

    def tear_down(self):
        for container_name in self.all_hosts():
            self._tear_down_container(container_name)
        self._remove_host_mount_dirs()
        if self.client:
            self.client.close()
            self.client = None

    def _tear_down_container(self, container_name):
        try:
            shutil.rmtree(self.get_dist_dir(unique=True))
        except OSError as e:
            # no such file or directory
            if e.errno != errno.ENOENT:
                raise

        try:
            self.stop_host(container_name)
            self.client.remove_container(container_name, v=True, force=True)
        except APIError as e:
            # container does not exist
            if e.response.status_code != 404:
                raise

    def stop_host(self, container_name):
        self.client.stop(container_name)
        self.client.wait(container_name)

    def start_host(self, container_name):
        self.client.start(container_name)

    def get_down_hostname(self, host_name):
        return host_name

    def _remove_host_mount_dirs(self):
        for container_name in self.all_hosts():
            try:
                shutil.rmtree(
                    self.get_local_mount_dir(container_name))
            except OSError as e:
                # no such file or directory
                if e.errno != errno.ENOENT:
                    raise

    def _create_host_mount_dirs(self):
        for container_name in self.all_hosts():
            try:
                os.makedirs(
                    self.get_local_mount_dir(container_name))
            except OSError as e:
                # file exists
                if e.errno != errno.EEXIST:
                    raise

    @staticmethod
    def _execute_and_wait(func, *args, **kwargs):
        ret = func(*args, **kwargs)
        # go through all lines in returned stream to ensure func finishes
        output = ''
        for line in ret:
            output += line
        return output

    def _create_and_start_containers(self, master_image, slave_image=None,
                                     cmd=None, **kwargs):
        if slave_image:
            for container_name in self.slaves:
                container_mount_dir = \
                    self.get_local_mount_dir(container_name)
                self._create_container(
                    slave_image, container_name,
                    container_name.split('-')[0], cmd
                )
                self.client.start(container_name,
                                  binds={container_mount_dir:
                                         {'bind': self.mount_dir,
                                          'ro': False}},
                                  **kwargs)

        master_mount_dir = self.get_local_mount_dir(self.master)
        self._create_container(
            master_image, self.master, hostname=self.internal_master,
            cmd=cmd
        )
        self.client.start(self.master,
                          binds={master_mount_dir:
                                 {'bind': self.mount_dir,
                                  'ro': False}},
                          links=zip(self.slaves, self.slaves), **kwargs)
        self._add_hostnames_to_slaves()

    def _create_container(self, image, container_name, hostname=None,
                          cmd=None):
        self._execute_and_wait(self.client.create_container,
                               image,
                               detach=True,
                               name=container_name,
                               hostname=hostname,
                               volumes=self.local_mount_dir,
                               command=cmd,
                               host_config={'mem_limit': '2g'})

    def _add_hostnames_to_slaves(self):
        ips = self.get_ip_address_dict()
        additions_to_etc_hosts = ''
        for host in self.all_internal_hosts():
            additions_to_etc_hosts += '%s\t%s\n' % (ips[host], host)

        for host in self.slaves:
            self.exec_cmd_on_host(
                host,
                'bin/bash -c \'echo "%s" >> /etc/hosts\''
                % additions_to_etc_hosts
            )

    def _ensure_docker_containers_started(self, image):
        centos_based_images = [BASE_TD_IMAGE_NAME]

        timeout = 0
        is_host_started = {}
        for host in self.all_hosts():
            is_host_started[host] = False
        while timeout < self._DOCKER_START_TIMEOUT:
            for host in self.all_hosts():
                atomic_is_started = True
                atomic_is_started &= \
                    self.client.inspect_container(host)['State']['Running']
                if image in centos_based_images or \
                        image.startswith(self.IMAGE_NAME_BASE):
                    atomic_is_started &= \
                        self._are_centos_container_services_up(host)
                is_host_started[host] = atomic_is_started
            if not DockerCluster._are_all_hosts_started(is_host_started):
                timeout += 1
                sleep(1)
            else:
                break
        if timeout is self._DOCKER_START_TIMEOUT:
            raise DockerClusterException(
                'Docker container timed out on start.' + str(is_host_started))

    @staticmethod
    def _are_all_hosts_started(host_started_map):
        all_started = True
        for host in host_started_map.keys():
            all_started &= host_started_map[host]
        return all_started

    def _are_centos_container_services_up(self, host):
        """Some essential services in our CentOS containers take some time
        to start after the container itself is up. This function checks
        whether those services are up and returns a boolean accordingly.
        Specifically, we check that the app-admin user has been created
        and that the ssh daemon is up, as well as that the SSH keys are
        in the right place.

        Args:
          host: the host to check.

        Returns:
          True if the specified services have started, False otherwise.

        """
        ps_output = self.exec_cmd_on_host(host, 'ps')
        # also ensure that the app-admin user exists
        try:
            user_output = self.exec_cmd_on_host(
                host, 'grep app-admin /etc/passwd'
            )
            user_output += self.exec_cmd_on_host(host, 'stat /home/app-admin')
        except OSError:
            user_output = ''
        if 'sshd_bootstrap' in ps_output or 'sshd\n' not in ps_output\
                or not user_output:
            return False
        # check for .ssh being in the right place
        try:
            ssh_output = self.exec_cmd_on_host(host, 'ls /home/app-admin/.ssh')
            if 'id_rsa' not in ssh_output:
                return False
        except OSError:
            return False
        return True

    def exec_cmd_on_host(self, host, cmd, user=None, raise_error=True,
                         tty=False):
        ex = self.client.exec_create(self.__get_unique_host(host), cmd,
                                     tty=tty, user=user)
        output = self.client.exec_start(ex['Id'], tty=tty)
        exit_code = self.client.exec_inspect(ex['Id'])['ExitCode']
        if raise_error and exit_code:
            raise OSError(exit_code, output)
        return output

    @staticmethod
    def _get_tag_basename(bare_image_provider, cluster_type, ms):
        return '_'.join(
            [bare_image_provider.get_tag_decoration(), cluster_type, ms])

    @staticmethod
    def _get_master_image_name(bare_image_provider, cluster_type):
        return os.path.join(DockerCluster.IMAGE_NAME_BASE,
                            DockerCluster._get_tag_basename(
                                bare_image_provider, cluster_type, 'master'))

    @staticmethod
    def _get_slave_image_name(bare_image_provider, cluster_type):
        return os.path.join(DockerCluster.IMAGE_NAME_BASE,
                            DockerCluster._get_tag_basename(
                                bare_image_provider, cluster_type, 'slave'))

    @staticmethod
    def _get_image_names(bare_image_provider, cluster_type):
        dc = DockerCluster
        return (dc._get_master_image_name(bare_image_provider, cluster_type),
                dc._get_slave_image_name(bare_image_provider, cluster_type))

    @staticmethod
    def start_cluster(bare_image_provider, cluster_type, master_host='master',
                      slave_hosts=None, **kwargs):
        if slave_hosts is None:
            slave_hosts = ['slave1', 'slave2', 'slave3']
        created_bare = False
        dc = DockerCluster

        centos_cluster = DockerCluster(master_host, slave_hosts,
                                       DEFAULT_LOCAL_MOUNT_POINT,
                                       DEFAULT_DOCKER_MOUNT_POINT)

        master_name, slave_name = dc._get_image_names(
            bare_image_provider, cluster_type)

        if not dc._check_for_images(master_name, slave_name):
            master_name, slave_name = dc._get_image_names(
                bare_image_provider, dc.BARE_CLUSTER_TYPE)
            if not dc._check_for_images(master_name, slave_name):
                bare_image_provider.create_bare_images(
                    centos_cluster, master_name, slave_name)
            created_bare = True

        centos_cluster.start_containers(master_name, slave_name, **kwargs)

        return centos_cluster, created_bare

    @staticmethod
    def _check_for_images(master_image_name, slave_image_name, tag='latest'):
        master_repotag = '%s:%s' % (master_image_name, tag)
        slave_repotag = '%s:%s' % (slave_image_name, tag)
        with Client(timeout=180) as client:
            images = client.images()
        has_master_image = False
        has_slave_image = False
        for image in images:
            if master_repotag in image['RepoTags']:
                has_master_image = True
            if slave_repotag in image['RepoTags']:
                has_slave_image = True
        return has_master_image and has_slave_image

    def commit_images(self, bare_image_provider, cluster_type):
        self.client.commit(self.master,
                           self._get_master_image_name(bare_image_provider,
                                                       cluster_type))
        if self.slaves:
            self.client.commit(self.slaves[0],
                               self._get_slave_image_name(bare_image_provider,
                                                          cluster_type))

    def run_script_on_host(self, script_contents, host):
        temp_script = '/tmp/tmp.sh'
        self.write_content_to_host('#!/bin/bash\n%s' % script_contents,
                                   temp_script, host)
        self.exec_cmd_on_host(host, 'chmod +x %s' % temp_script)
        return self.exec_cmd_on_host(host, temp_script, tty=True)

    def write_content_to_host(self, content, path, host):
        filename = os.path.basename(path)
        dest_dir = os.path.dirname(path)
        host_local_mount_point = self.get_local_mount_dir(host)
        local_path = os.path.join(host_local_mount_point, filename)

        with open(local_path, 'w') as config_file:
            config_file.write(content)

        self.exec_cmd_on_host(host, 'mkdir -p ' + dest_dir)
        self.exec_cmd_on_host(
            host, 'cp %s %s' % (os.path.join(self.mount_dir, filename),
                                dest_dir))

    def copy_to_host(self, source_path, dest_host, **kwargs):
        shutil.copy(source_path, self.get_local_mount_dir(dest_host))

    def get_ip_address_dict(self):
        ip_addresses = {}
        for host, internal_host in zip(self.all_hosts(),
                                       self.all_internal_hosts()):
            inspect = self.client.inspect_container(host)
            ip_addresses[host] = inspect['NetworkSettings']['IPAddress']
            ip_addresses[internal_host] = \
                inspect['NetworkSettings']['IPAddress']
        return ip_addresses

    def _post_presto_install(self):
        for worker in self.slaves:
            self.run_script_on_host(
                'sed -i /node.id/d /etc/presto/node.properties; '
                'uuid=$(uuidgen); '
                'echo node.id=$uuid >> /etc/presto/node.properties',
                worker
            )

    def postinstall(self, installer):
        from tests.product.standalone.presto_installer \
            import StandalonePrestoInstaller

        _post_install_hooks = {
            StandalonePrestoInstaller: DockerCluster._post_presto_install
        }

        hook = _post_install_hooks.get(installer, None)
        if hook:
            hook(self)


class DockerClusterException(Exception):
    def __init__(self, msg):
        self.msg = msg
