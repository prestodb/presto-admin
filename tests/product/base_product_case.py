import os
import shutil
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
        self.create_docker_cluster()

    def tearDown(self):
        self.restore_stdout_stderr()
        self.tear_down_docker_cluster()

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

    def test_setup_teardown(self):
        pass
