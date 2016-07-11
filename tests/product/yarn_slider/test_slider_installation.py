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
Product tests for installing Apache Slider
"""

from nose.plugins.attrib import attr

from tests.no_hadoop_bare_image_provider import NoHadoopBareImageProvider
from tests.product.base_product_case import BaseProductTestCase
from tests.product.cluster_types import YARN_SLIDER_PA_CLUSTER
from tests.product.yarn_slider.slider_installer import SliderInstaller
from tests.product.yarn_slider.pa_slider_config import cluster_config, \
    docker_config, get_config, upload_config

from prestoadmin.yarn_slider.config import HOST, DIR, SLIDER_USER, \
    _SLIDER_CONFIG


class TestSliderInstallation(BaseProductTestCase):
    def setUp(self):
        super(TestSliderInstallation, self).setUp()
        self.setup_cluster(NoHadoopBareImageProvider(), YARN_SLIDER_PA_CLUSTER)

    def _get_interactive_config(self, conf):
        cluster_conf = cluster_config(conf)
        expect = ''

        prompts = []
        for item in _SLIDER_CONFIG:
            item.collect_prompts(prompts)

        for (prompt, key) in prompts:
            expect += 'expect "%s"\nsend "%s\\n"\n' % (prompt,
                                                       cluster_conf[key])

        expect += 'expect eof'
        return expect

    def ensure_slider_user_exists(self, conf):
        self.cluster.run_script_on_host(
            'getent passwd %(user)s >/dev/null || adduser %(user)s'
            % ({'user': conf[SLIDER_USER]}), conf[HOST])

    def assert_slider_not_installed(self, conf):
        docker_conf = docker_config(conf)
        self.assert_path_removed(docker_conf[HOST], docker_conf[DIR])

    def run_with_config(self, conf):
        conf = upload_config(self.cluster, conf)
        self.assert_slider_not_installed(conf)

        slider_path = SliderInstaller.copy_slider_dist_to_cluster(self)
        SliderInstaller.install_slider_package(self, slider_path)

        SliderInstaller.assert_installed(self, conf)

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
        self.assert_slider_not_installed(conf)

        self.ensure_slider_user_exists(conf)
        slider_path = SliderInstaller.copy_slider_dist_to_cluster(self)

        self.run_prestoadmin_expect('slider install %s' %
                                    (slider_path,), expect)

        SliderInstaller.assert_installed(self, conf)

    @attr('smoketest')
    def test_slider_install(self):
        self.run_with_config(get_config())

    def test_slider_install_interactive(self):
        self.run_interactive(get_config())

    def test_slider_install_localhost(self):
        self.run_with_config(
            get_config(override={HOST: ('localhost', self.cluster.master)}))

    def test_slider_install_interactive_ip(self):
        ips = self.cluster.get_ip_address_dict()
        self.run_interactive(
            get_config(override={HOST: (ips[self.cluster.master],
                                        self.cluster.master)}))
