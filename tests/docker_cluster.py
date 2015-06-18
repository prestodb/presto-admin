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
from time import sleep

from docker import Client
from docker.errors import APIError

INSTALLED_PRESTO_TEST_MASTER_IMAGE = 'teradatalabs/centos-presto-test-master'
INSTALLED_PRESTO_TEST_SLAVE_IMAGE = 'teradatalabs/centos-presto-test-slave'


class DockerCluster(object):
    """Start/stop/control/query arbitrary clusters of docker containers.

    This class is aimed at product test writers to create docker containers
    for testing purposes.

    """
    def __init__(self, master_host, slave_hosts):
        self.master_host = master_host
        self.slave_hosts = slave_hosts
        self.client = Client(timeout=180)
        self._DOCKER_START_TIMEOUT = 30
        DockerCluster.check_if_docker_exists()

    def all_hosts(self):
        return self.slave_hosts + [self.master_host]

    @staticmethod
    def check_if_docker_exists():
        try:
            subprocess.call(['docker', '--version'])
        except OSError:
            sys.exit('Docker is not installed. Try installing it with '
                     'presto-admin/bin/install-docker.sh.')

    def create_image(self, path_to_dockerfile_dir, image_tag, base_image):
        self.fetch_image_if_not_present(base_image)

        self._execute_and_wait(self.client.build,
                               path=path_to_dockerfile_dir,
                               tag=image_tag,
                               rm=True)

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

    def start_containers(self, local_mount_dir, docker_mount_dir,
                         master_image, slave_image=None, cmd=None):
        self.tear_down_containers(local_mount_dir)
        self._create_host_mount_dirs(local_mount_dir)

        self._create_and_start_containers(local_mount_dir, docker_mount_dir,
                                          master_image, slave_image, cmd)
        self._ensure_docker_containers_started(master_image)

    def tear_down_containers(self, local_mount_dir):
        for container_name in self.all_hosts():
            self._tear_down_container(container_name)
        self._remove_host_mount_dirs(local_mount_dir)

    def _tear_down_container(self, container_name):
        try:
            self.stop_container_and_wait(container_name)
            self.client.remove_container(container_name, v=True)
        except APIError as e:
            # container does not exist
            if e.response.status_code != 404:
                raise

    def stop_container_and_wait(self, container_name):
        self.client.stop(container_name)
        self.client.wait(container_name)

    def _remove_host_mount_dirs(self, local_mount_dir):
        for container_name in self.all_hosts():
            try:
                shutil.rmtree(os.path.join(local_mount_dir, container_name))
            except OSError as e:
                # no such file or directory
                if e.errno != errno.ENOENT:
                    raise
        try:
            shutil.rmtree(local_mount_dir)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise

    def _create_host_mount_dirs(self, local_mount_dir):
        for container_name in self.all_hosts():
            try:
                os.makedirs(os.path.join(local_mount_dir, container_name))
            except OSError as e:
                # file exists
                if e.errno != errno.EEXIST:
                    raise

    @staticmethod
    def _execute_and_wait(func, *args, **kwargs):
        ret = func(*args, **kwargs)
        # go through all lines in returned stream to ensure func finishes
        for line in ret:
            pass

    def _create_and_start_containers(self, local_mount_dir, docker_mount_dir,
                                     master_image, slave_image=None, cmd=None):
        if slave_image:
            for container_name in self.slave_hosts:
                container_mount_dir = os.path.join(local_mount_dir,
                                                   container_name)
                self._create_container(slave_image, container_name,
                                       container_mount_dir, cmd=cmd)
                self.client.start(container_name,
                                  binds={container_mount_dir:
                                         {'bind': docker_mount_dir,
                                          'ro': False}})

        master_mount_dir = os.path.join(local_mount_dir, self.master_host)
        self._create_container(
            master_image, self.master_host, master_mount_dir,
            hostname=self.master_host, cmd=cmd
        )
        self.client.start(self.master_host,
                          binds={master_mount_dir:
                                 {'bind': docker_mount_dir,
                                  'ro': False}},
                          links=zip(self.slave_hosts, self.slave_hosts))

    def _create_container(self, image, container_name, local_mount_dir,
                          hostname=None, cmd=None):
        self._execute_and_wait(self.client.create_container,
                               image,
                               detach=True,
                               name=container_name,
                               hostname=hostname,
                               volumes=local_mount_dir,
                               command=cmd)

    def _ensure_docker_containers_started(self, image):
        centos_based_images = ['teradatalabs/centos6-ssh-test',
                               INSTALLED_PRESTO_TEST_MASTER_IMAGE,
                               INSTALLED_PRESTO_TEST_SLAVE_IMAGE]
        timeout = 0
        is_host_started = {}
        for host in self.all_hosts():
            is_host_started[host] = False
        while timeout < self._DOCKER_START_TIMEOUT:
            for host in self.all_hosts():
                atomic_is_started = True
                atomic_is_started &= \
                    self.client.inspect_container(host)['State']['Running']
                if image in centos_based_images:
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
        and that the ssh daemon is up.

        Args:
          host: the host to check.

        Returns:
          True if the specified services have started, False otherwise.

        """
        ps_output = self.exec_cmd_on_container(host, 'ps')
        # also ensure that the app-admin user exists
        try:
            user_output = self.exec_cmd_on_container(
                host, 'grep app-admin /etc/passwd'
            )
        except OSError:
            user_output = ''
        if 'sshd_bootstrap' in ps_output or 'sshd\n' not in ps_output\
                or not user_output:
            return False
        return True

    def exec_cmd_on_container(self, host, cmd, raise_error=True, tty=False):
        ex = self.client.exec_create(host, cmd, tty=tty)
        output = self.client.exec_start(ex['Id'], tty=tty)
        exit_code = self.client.exec_inspect(ex['Id'])['ExitCode']
        if raise_error and exit_code:
            raise OSError(exit_code, output)
        return output


class DockerClusterException(Exception):
    def __init__(self, msg):
        self.msg = msg
