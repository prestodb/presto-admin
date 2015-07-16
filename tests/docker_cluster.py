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
from nose.tools import nottest

from prestoadmin import main_dir

INSTALLED_PRESTO_TEST_MASTER_IMAGE = 'teradatalabs/centos-presto-test-master'
INSTALLED_PRESTO_TEST_SLAVE_IMAGE = 'teradatalabs/centos-presto-test-slave'
DEFAULT_DOCKER_MOUNT_POINT = '/mnt/presto-admin'
DEFAULT_LOCAL_MOUNT_POINT = os.path.join(main_dir, 'tmp/docker-pa/')
LOCAL_RESOURCES_DIR = os.path.join(main_dir, 'tests/product/resources/')
DIST_DIR = os.path.join(main_dir, 'tmp/installer')


class DockerCluster(object):
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
        self.client = Client(timeout=180)
        self._DOCKER_START_TIMEOUT = 30
        DockerCluster.__check_if_docker_exists()

    def all_hosts(self):
        return self.slaves + [self.master]

    def all_internal_hosts(self):
        """The difference between this method and all_hosts() is that
        all_hosts() returns the unique, "outside facing" hostnames that
        docker uses. On the other hand all_internal_hosts() returns the
        more human readable host aliases for the containers used internally
        between containers. For example the unique master host will
        look something like 'master-07d1774e-72d7-45da-bf84-081cfaa5da9a',
        whereas the internal master host will be 'master'.

        Returns:
            List of all internal hosts with the random suffix stripped out.
        """
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

    def start_containers(self, master_image, slave_image=None,
                         cmd=None, **kwargs):
        self.tear_down()
        self._create_host_mount_dirs()

        self._create_and_start_containers(master_image, slave_image,
                                          cmd, **kwargs)
        self._ensure_docker_containers_started(master_image)

    def tear_down(self):
        for container_name in self.all_hosts():
            self._tear_down_container(container_name)
        self._remove_host_mount_dirs()

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
        for line in ret:
            pass

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

    def _create_container(self, image, container_name, hostname=None,
                          cmd=None):
        self._execute_and_wait(self.client.create_container,
                               image,
                               detach=True,
                               name=container_name,
                               hostname=hostname,
                               volumes=self.local_mount_dir,
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
        return True

    def exec_cmd_on_host(self, host, cmd, raise_error=True, tty=False):
        ex = self.client.exec_create(self.__get_unique_host(host), cmd,
                                     tty=tty)
        output = self.client.exec_start(ex['Id'], tty=tty)
        exit_code = self.client.exec_inspect(ex['Id'])['ExitCode']
        if raise_error and exit_code:
            raise OSError(exit_code, output)
        return output

    @staticmethod
    def start_presto_cluster(install_func):
        if DockerCluster._check_for_presto_images():
            presto_cluster = DockerCluster('master',
                                           ['slave1', 'slave2', 'slave3'],
                                           DEFAULT_LOCAL_MOUNT_POINT,
                                           DEFAULT_DOCKER_MOUNT_POINT)
            presto_cluster.start_containers(INSTALLED_PRESTO_TEST_MASTER_IMAGE,
                                            INSTALLED_PRESTO_TEST_SLAVE_IMAGE)
            return presto_cluster
        else:
            presto_cluster = DockerCluster.start_base_cluster()
            install_func(presto_cluster)
            presto_cluster.client.commit(
                presto_cluster.master,
                INSTALLED_PRESTO_TEST_MASTER_IMAGE
            )
            presto_cluster.client.commit(
                presto_cluster.slaves[0],
                INSTALLED_PRESTO_TEST_SLAVE_IMAGE
            )
            return presto_cluster

    @staticmethod
    def start_base_cluster():
        centos_cluster = DockerCluster('master',
                                       ['slave1', 'slave2', 'slave3'],
                                       DEFAULT_LOCAL_MOUNT_POINT,
                                       DEFAULT_DOCKER_MOUNT_POINT)
        centos_cluster.create_image(
            os.path.join(LOCAL_RESOURCES_DIR, 'centos6-ssh-test'),
            'teradatalabs/centos6-ssh-test',
            'jdeathe/centos-ssh'
        )
        centos_cluster.start_containers(
            'teradatalabs/centos6-ssh-test',
            'teradatalabs/centos6-ssh-test'
        )
        return centos_cluster

    @staticmethod
    def _check_for_presto_images():
        client = Client(timeout=180)
        images = client.images()
        has_master_image = False
        has_slave_image = False
        for image in images:
            if INSTALLED_PRESTO_TEST_MASTER_IMAGE in image['RepoTags'][0]:
                has_master_image = True
            if INSTALLED_PRESTO_TEST_SLAVE_IMAGE in image['RepoTags'][0]:
                has_slave_image = True
        return has_master_image and has_slave_image

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

    def copy_to_host(self, source_path, dest_host):
        shutil.copy(source_path, self.get_local_mount_dir(dest_host))

    @nottest
    def clean_up_presto_test_images(self):
        try:
            self.client.remove_image(INSTALLED_PRESTO_TEST_MASTER_IMAGE)
            self.client.remove_image(INSTALLED_PRESTO_TEST_SLAVE_IMAGE)
        except:
            pass

    def get_ip_address_dict(self):
        ip_addresses = {}
        for host, internal_host in zip(self.all_hosts(),
                                       self.all_internal_hosts()):
            inspect = self.client.inspect_container(host)
            ip_addresses[host] = inspect['NetworkSettings']['IPAddress']
            ip_addresses[internal_host] = \
                inspect['NetworkSettings']['IPAddress']
        return ip_addresses


class DockerClusterException(Exception):
    def __init__(self, msg):
        self.msg = msg
