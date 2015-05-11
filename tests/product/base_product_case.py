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

import distutils.core
import fnmatch
import json
import os
import shutil
import subprocess
import sys
from docker.errors import APIError
import errno
import prestoadmin
from docker import Client
from tests import utils

LOCAL_MOUNT_POINT = os.path.join(prestoadmin.main_dir, "tmp/docker-pa/%s")
DOCKER_MOUNT_POINT = "/mnt/presto-admin"


class BaseProductTestCase(utils.BaseTestCase):
    client = Client()
    slaves = ["slave1", "slave2", "slave3"]
    master = "master"

    def setUp(self):
        self.capture_stdout_stderr()
        self.check_if_docker_exists()
        self.create_docker_cluster()

    def tearDown(self):
        self.restore_stdout_stderr_keep_open()
        self.tear_down_docker_cluster()

    def check_if_docker_exists(self):
        try:
            subprocess.call(['docker'])
        except OSError:
            sys.exit('Docker is not installed. Try installing it with '
                     'presto-admin/bin/install-docker.sh.')

    def create_host_mount_dirs(self):
        for container_name in [self.master] + self.slaves:
            try:
                os.makedirs(LOCAL_MOUNT_POINT % container_name)
            except OSError as e:
                # file exists
                if e.errno == errno.EEXIST:
                    pass

    def create_docker_cluster(self):
        self.tear_down_docker_cluster()
        self.create_host_mount_dirs()

        if not self.client.images("jdeathe/centos-ssh"):
            self._execute_and_wait(self.client.pull, "jdeathe/centos-ssh")
        self._execute_and_wait(self.client.build,
                               path=os.path.join(prestoadmin.main_dir,
                                                 "tests/product/resources/"
                                                 "centos6-ssh-test"),
                               tag="teradatalabs/centos6-ssh-test", rm=True)

        for container_name in self.slaves:
            self._execute_and_wait(self.client.create_container,
                                   "teradatalabs/centos6-ssh-test",
                                   detach=True,
                                   name=container_name,
                                   volumes=LOCAL_MOUNT_POINT %
                                   container_name)

            self.client.start(container_name,
                              binds={LOCAL_MOUNT_POINT % container_name:
                                     {"bind": DOCKER_MOUNT_POINT,
                                      "ro": False}})

        self._execute_and_wait(self.client.create_container,
                               "teradatalabs/centos6-ssh-test",
                               detach=True,
                               name=self.master,
                               volumes=LOCAL_MOUNT_POINT % self.master)

        self.client.start(self.master,
                          binds={LOCAL_MOUNT_POINT % self.master:
                                 {"bind": DOCKER_MOUNT_POINT,
                                  "ro": False}},
                          links=zip(self.slaves, self.slaves))

    def _execute_and_wait(self, func, *args, **kwargs):
        ret = func(*args, **kwargs)
        # go through all lines in returned stream to ensure func finishes
        for line in ret:
            pass

    def remove_host_mount_dirs(self):
        for container_name in [self.master] + self.slaves:
            try:
                shutil.rmtree(LOCAL_MOUNT_POINT % container_name)
            except OSError as e:
                # no such file or directory
                if e.errno == errno.ENOENT:
                    pass

        try:
            os.removedirs(os.path.dirname(LOCAL_MOUNT_POINT))
        except OSError as e:
            if e.errno == errno.ENOENT:
                pass

    def tear_down_docker_cluster(self):
        for container in [self.master] + self.slaves:
            try:
                self.client.stop(container)
                self.client.wait(container)
                self.client.remove_container(container)
            except APIError as e:
                # container does not exist
                if e.response.status_code == 404:
                    pass

        self.remove_host_mount_dirs()

    def copy_to_master(self, source):
        shutil.copy(source, LOCAL_MOUNT_POINT % self.master)

    def install_presto_admin(self):
        dist_dir = os.path.join(prestoadmin.main_dir, "dist")
        if not os.path.exists(dist_dir) or not fnmatch.filter(
                os.listdir(dist_dir), "prestoadmin-*.tar.bz2"):
            # setup.py expects you to be in the main directory
            saved_path = os.getcwd()
            os.chdir(prestoadmin.main_dir)
            distutils.core.run_setup("setup.py",
                                     ["bdist_prestoadmin"]).run_commands()
            os.chdir(saved_path)
        for dist_file in os.listdir(dist_dir):
            if fnmatch.fnmatch(dist_file, "prestoadmin-*.tar.bz2"):
                self.copy_to_master(os.path.join(dist_dir, dist_file))
        self.copy_to_master(prestoadmin.main_dir +
                            "/tests/product/resources/install-admin.sh")
        self.exec_create_start(self.master,
                               DOCKER_MOUNT_POINT + "/install-admin.sh")

    def exec_create_start(self, host, command):
        ex = self.client.exec_create(host, command)
        return self.client.exec_start(ex['Id'])

    def upload_topology(self, topology=None):
        if not topology:
            topology = {"coordinator": "master",
                        "workers": ["slave1", "slave2", "slave3"]}
        with open(os.path.join(LOCAL_MOUNT_POINT % self.master,
                               "config.json"), "w") as conf_file:
            json.dump(topology, conf_file)

        self.exec_create_start(self.master, "cp %s /etc/opt/presto-admin/" %
                               os.path.join(DOCKER_MOUNT_POINT, "config.json"))

    def run_prestoadmin(self, command):
        return self.exec_create_start(self.master,
                                      "/opt/prestoadmin/presto-admin %s"
                                      % command)
