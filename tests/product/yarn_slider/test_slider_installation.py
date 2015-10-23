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

from tests.product.base_product_case import BaseProductTestCase
from tests.product.constants import LOCAL_RESOURCES_DIR

from prestoadmin.slider.config import SLIDER_CONFIG_PATH, HOST, DIR, \
    SLIDER_USER, _SLIDER_CONFIG

SLIDER_DIST_FILENAME = 'slider-assembly-0.80.0-incubating-all.tar.gz'


class TestSliderInstallation(BaseProductTestCase):
    def setUp(self):
        super(TestSliderInstallation, self).setUp()
        # For the moment, we still need to topology loaded. This will change
        # when we implement mode switching, and then these tests should be
        # updated to run on a PA_ONLY_CLUSTER
        self.setup_cluster(self.PA_ONLY_CLUSTER)

    def get_config(self, override=None):
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

    def pick_config(self, conf, pick):
        result = {}
        for key in conf:
            value = conf[key]
            if isinstance(value, tuple):
                cluster_value, docker_value = value
                result[key] = pick(cluster_value, docker_value)
            else:
                result[key] = value

        return result

    def cluster_config(self, conf):
        def pick(cluster_value, docker_value):
            return cluster_value
        return self.pick_config(conf, pick)

    def docker_config(self, conf):
        def pick(cluster_value, docker_value):
            return docker_value
        return self.pick_config(conf, pick)

    def upload_config(self, conf):
        self.cluster.write_content_to_host(
            json.dumps(self.cluster_config(conf)), SLIDER_CONFIG_PATH,
            self.cluster.master)
        return conf

    def get_interactive_config(self, conf):
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

    def copy_slider_dist_to_cluster(self):
        slider_filename = SLIDER_DIST_FILENAME
        slider_path = os.path.join(LOCAL_RESOURCES_DIR, slider_filename)
        self.cluster.copy_to_host(slider_path, self.cluster.master)
        return os.path.join(self.cluster.mount_dir, slider_filename)

    def ensure_slider_user_exists(self, conf):
        self.cluster.run_script_on_host(
            'getent passwd %(user)s >/dev/null || adduser %(user)s'
            % ({'user': conf[SLIDER_USER]}), conf[HOST])

    def assert_slider_not_installed(self, conf):
        docker_conf = self.docker_config(conf)
        self.assert_path_removed(docker_conf[HOST], docker_conf[DIR])

    def assert_slider_installed(self, conf):
        docker_conf = self.docker_config(conf)
        self.assert_path_exists(docker_conf[HOST],
                                os.path.join(docker_conf[DIR], 'LICENSE'))

    def run_with_config(self, conf):
        conf = self.upload_config(conf)
        self.assert_slider_not_installed(conf)

        slider_path = self.copy_slider_dist_to_cluster()
        self.run_prestoadmin(
            'slider slider_install %s' %
            (os.path.join(self.cluster.mount_dir, slider_path)))

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
        expect = self.get_interactive_config(conf)
        self.assert_slider_not_installed(conf)

        self.ensure_slider_user_exists(conf)
        slider_path = self.copy_slider_dist_to_cluster()

        self.run_prestoadmin_expect('slider slider_install %s' % (slider_path),
                                    expect)

        self.assert_slider_installed(conf)

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
