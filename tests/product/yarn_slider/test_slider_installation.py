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
Product tests for installing slider itself
"""

import json
import os

from nose.plugins.attrib import attr

from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase
from tests.product.constants import LOCAL_RESOURCES_DIR

from prestoadmin.slider.config import SLIDER_CONFIG_PATH, HOST, DIR, \
    SLIDER_USER, _SLIDER_CONFIG

SLIDER_DIST_FILENAME = 'slider-assembly-0.80.0-incubating-all.tar.gz'


class TestSliderInstallation(BaseProductTestCase):
    def setUp(self):
        super(TestSliderInstallation, self).setUp()
        self.setup_cluster(NoHadoopBareImageProvider(), self.PA_ONLY_CLUSTER)

    @staticmethod
    def get_config(override=None):
        conf = {
            'slider_directory': '/opt/slider',
            'admin': 'root',
            'HADOOP_CONF': '/etc/hadoop/conf',
            'ssh_port': 22,
            'slider_user': 'yarn',
            'slider_master': 'master',
            'JAVA_HOME': '/usr/lib/jvm/java'
        }

        if override:
            conf.update(override)
        return conf

    @staticmethod
    def pick_config(conf, pick):
        result = {}
        for key in conf:
            value = conf[key]
            if isinstance(value, tuple):
                cluster_value, docker_value = value
                result[key] = pick(cluster_value, docker_value)
            else:
                result[key] = value

        return result

    @staticmethod
    def cluster_config(conf):
        def pick(cluster_value, docker_value):
            return cluster_value
        return TestSliderInstallation.pick_config(conf, pick)

    @staticmethod
    def docker_config(conf):
        def pick(cluster_value, docker_value):
            return docker_value
        return TestSliderInstallation.pick_config(conf, pick)

    @staticmethod
    def upload_config(cluster, conf):
        cluster.write_content_to_host(
            json.dumps(TestSliderInstallation.cluster_config(conf)),
            SLIDER_CONFIG_PATH, cluster.master)
        return conf

    def _upload_config(self, conf):
        return TestSliderInstallation.upload_config(self.cluster, conf)

    def _get_interactive_config(self, conf):
        cluster_conf = self.cluster_config(conf)
        expect = ''

        prompts = []
        for item in _SLIDER_CONFIG:
            item.collect_prompts(prompts)

        for (prompt, key) in prompts:
            expect += 'expect "%s"\nsend "%s\\n"\n' % (prompt,
                                                       cluster_conf[key])

        expect += 'expect eof'
        return expect

    @staticmethod
    def copy_slider_dist_to_cluster(testcase):
        slider_filename = SLIDER_DIST_FILENAME
        slider_path = os.path.join(LOCAL_RESOURCES_DIR, slider_filename)
        testcase.cluster.copy_to_host(slider_path, testcase.cluster.master)
        return os.path.join(testcase.cluster.mount_dir, slider_filename)

    @staticmethod
    def install_slider_package(testcase, slider_path):
        testcase.run_prestoadmin(
            'slider slider_install %s' %
            (os.path.join(testcase.cluster.mount_dir, slider_path)))

    def ensure_slider_user_exists(self, conf):
        self.cluster.run_script_on_host(
            'getent passwd %(user)s >/dev/null || adduser %(user)s'
            % ({'user': conf[SLIDER_USER]}), conf[HOST])

    @staticmethod
    def assert_slider_not_installed(testcase, conf):
        docker_conf = testcase.docker_config(conf)
        testcase.assert_path_removed(docker_conf[HOST], docker_conf[DIR])

    def assert_slider_installed(self, conf):
        docker_conf = self.docker_config(conf)
        self.assert_path_exists(docker_conf[HOST],
                                os.path.join(docker_conf[DIR], 'LICENSE'))

    def run_with_config(self, conf):
        conf = self._upload_config(conf)
        self.assert_slider_not_installed(self, conf)

        slider_path = self.copy_slider_dist_to_cluster(self)
        self.install_slider_package(self, slider_path)

        self.assert_slider_installed(conf)

    """
    Sad story time: In other places we echo the responses to the interactive
    prompts into the presto-admin process through a pipe. Unfortunately, when
    we tried that here, a responses to prompts that follow a long-running
    validator were truncated at the begining. I.e. a response of abcde was read
    as cde. It's not clear if this is happening because of some weird buffering
    issue, or if this is happening because fabric is doing something weird with
    stdin/stdout when you call run or sudo.

    Whatever the cause, running the interactive stuff via expect avoids the
    problem entirely.
    """
    def run_interactive(self, conf):
        expect = self._get_interactive_config(conf)
        self.assert_slider_not_installed(self, conf)

        self.ensure_slider_user_exists(conf)
        slider_path = self.copy_slider_dist_to_cluster(self)

        self.run_prestoadmin_expect('slider slider_install %s' % (slider_path),
                                    expect)

        self.assert_slider_installed(conf)

    @attr('smoketest')
    def test_slider_install(self):
        self.run_with_config(
            self.get_config())

    def test_slider_install_interactive(self):
        self.run_interactive(
            self.get_config())

    def test_slider_install_localhost(self):
        self.run_with_config(
            self.get_config(override={HOST: ('localhost',
                                             self.cluster.master)}))

    def test_slider_install_interactive_ip(self):
        ips = self.cluster.get_ip_address_dict()
        self.run_interactive(
            self.get_config(override={HOST: (ips[self.cluster.master],
                                             self.cluster.master)}))
