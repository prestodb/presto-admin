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

import json
import os

from tests.product.yarn_slider.pa_slider_config import get_config
from prestoadmin.util.slider import degarbage_json
from prestoadmin.yarn_slider.config import APP_INST_NAME, SLIDER_USER
from prestoadmin.yarn_slider.slider_application_configs import \
    AppConfigJson, ResourcesJson
from prestoadmin.yarn_slider.config import SLIDER_CONFIG_PATH, \
    SLIDER_CONFIG_DIR, DIR
from prestoadmin.yarn_slider.server import get_coordinator, get_workers, \
    get_slider_bin
from prestoadmin.yarn_slider.slider_exit_codes import EXIT_BAD_STATE, \
    EXIT_UNKNOWN_INSTANCE




from tests.hdp_bare_image_provider import HdpBareImageProvider

from tests.product.constants import LOCAL_RESOURCES_DIR
from tests.product.yarn_slider.yarn_slider_test_case import YarnSliderTestCase

SLIDER_CLIENT_XML = 'slider-client.xml'


class StillRunningException(Exception):
    pass


class TestControl(YarnSliderTestCase):
    def setUp(self):
        super(TestControl, self).setUp()
        self.setup_cluster(HdpBareImageProvider(), self.YS_PRESTO_CLUSTER)
        self.await_hdfs()
        # Sometimes hdfs comes up in safemode; sometimes it doesn't. As nice as
        # it would be to figure out why, we really just need it up enough to
        # run the tests.
        self.hdfs_unsafemode()

    def get_slider_config(self):
        config = self.cluster.exec_cmd_on_host(
            self.cluster.get_master(), 'cat %s' % (SLIDER_CONFIG_PATH,))
        return json.loads(config)

    def upload_slider_config(self, conf):
        slider_dir = conf[DIR]

        self.cluster.copy_to_host(
            os.path.join(LOCAL_RESOURCES_DIR, SLIDER_CLIENT_XML),
            self.cluster.get_master(),
            os.path.join(slider_dir, 'conf', SLIDER_CLIENT_XML))

    def get_slider_status(self, conf):
        slider_status = self.cluster.exec_cmd_on_host(
            self.cluster.get_master(), '%s status %s' % (
                get_slider_bin(conf), conf[APP_INST_NAME]),
            user=conf[SLIDER_USER])

        return json.loads(degarbage_json(slider_status))

    def await_up_presto_status(self, conf):
        def f():
            slider_status = self.get_slider_status(conf)
            get_coordinator(slider_status)
            get_workers(slider_status)
            return slider_status

        # If the app instance has started, but all of the component instances
        # haven't, we'll end up getting a KeyError when we try to get the
        # worker or coordinator component instances out of the JSON data.
        # Retry if we do.
        return self.retry(f, KeyError)


    def await_presto_down(self, conf):
        def f():
            try:
                self.get_slider_status(conf)
                raise StillRunningException()
            except OSError as e:
                return e.errno

        return self.retry(f, StillRunningException)

    def get_app_inst_state(self, conf):
        slider_list = self.cluster.exec_cmd_on_host(
            self.cluster.get_master(), '%s list %s' % (
                get_slider_bin(conf), conf[APP_INST_NAME]),
            user=conf[SLIDER_USER])

        lines = slider_list.split('\n')
        for line in lines:
            tokens = line.split()
            if tokens[0] == conf[APP_INST_NAME]:
                try:
                    return tokens[1]
                except IndexError:
                    # App instances that have been built but never started
                    # can be listed, but only have a value in the first column.
                    # Return a fake state to distinguish from not being able to
                    # find the app instance.
                    return 'BUILT'
        return None

    def upload_slider_jsons(self, appConfig, resources):
        self.cluster.copy_to_host(
            appConfig.get_config_path(), self.cluster.get_master(),
            os.path.join(SLIDER_CONFIG_DIR, 'appConfig.json'))
        self.cluster.copy_to_host(
            resources.get_config_path(), self.cluster.get_master(),
            os.path.join(SLIDER_CONFIG_DIR, 'resources.json'))

    def assert_presto_running(self, conf, resources):
        slider_status = self.await_up_presto_status(conf)
        coordinators = get_coordinator(slider_status)
        workers = get_workers(slider_status)

        self.assertEqual('RUNNING', self.get_app_inst_state(conf))
        # There are a lot of ways things can go wrong staring the server.
        # Asserting that we have the right number of coordinators and workers
        # is probably close enough to ensuring that everything is OK.
        self.assertEqual(
            resources.get_coordinator_instances(), len(coordinators))
        self.assertEqual(
            resources.get_worker_instances(), len(workers))
        return slider_status

    def assert_presto_finished(self, conf):
        self.assertEqual(EXIT_BAD_STATE, self.await_presto_down(conf))
        self.assertEqual('FINISHED', self.get_app_inst_state(conf))

    def _test_start_stop(self, appConfig, resources):
        conf = self.get_slider_config()
        self.upload_slider_config(conf)
        self.upload_slider_jsons(appConfig, resources)

        self.run_prestoadmin('server start')

        self.assert_presto_running(conf, resources)

        self.run_prestoadmin('server stop')

        self.assert_presto_finished(conf)

    def test_start_stop(self):
        appConfig = AppConfigJson(
            os.path.join(LOCAL_RESOURCES_DIR, 'appConfig-multi.json'), None)
        resources = ResourcesJson(
            os.path.join(LOCAL_RESOURCES_DIR, 'resources-multi.json'), None)

        self._test_start_stop(appConfig, resources)

    def test_create_stop_start(self):
        appConfig = AppConfigJson(
            os.path.join(LOCAL_RESOURCES_DIR, 'appConfig-multi.json'), None)
        resources = ResourcesJson(
            os.path.join(LOCAL_RESOURCES_DIR, 'resources-multi.json'), None)

        conf = self.get_slider_config()
        self.upload_slider_config(conf)
        self.upload_slider_jsons(appConfig, resources)

        self.run_prestoadmin('server create')

        self.assert_presto_running(conf, resources)

        self.run_prestoadmin('server stop')

        self.assert_presto_finished(conf)

        self.run_prestoadmin('server start')
        self.assert_presto_running(conf, resources)

    def test_build(self):
        appConfig = AppConfigJson(
            os.path.join(LOCAL_RESOURCES_DIR, 'appConfig-multi.json'), None)
        resources = ResourcesJson(
            os.path.join(LOCAL_RESOURCES_DIR, 'resources-multi.json'), None)

        conf = self.get_slider_config()
        self.upload_slider_config(conf)
        self.upload_slider_jsons(appConfig, resources)

        self.run_prestoadmin('server build')

        # Build, but never run app instances are in a weird limbo state where
        # querying status will result in an EXIT_UNKNOWN_INSTANCE exit code,
        # but list <app_inst_name> will return a row with only one token.
        # We fake a state out for that case in get_app_inst_state, and then
        # look for the fake state here.
        self.assertEqual(EXIT_UNKNOWN_INSTANCE, self.await_presto_down(conf))
        self.assertEqual('BUILT', self.get_app_inst_state(conf))

        self.run_prestoadmin('server start')

        self.assert_presto_running(conf, resources)

    def test_single_node(self):
        appConfig = AppConfigJson(
            os.path.join(LOCAL_RESOURCES_DIR, 'appConfig-single.json'), None)
        resources = ResourcesJson(
            os.path.join(LOCAL_RESOURCES_DIR, 'resources-single.json'), None)

        self._test_start_stop(appConfig, resources)
