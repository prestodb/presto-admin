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

import os

from overrides import overrides

from fabric.operations import prompt
from fabric.state import env

from prestoadmin.util.base_config import JsonFileFromDefaultFile, \
    PresentFileConfig, RequireableConfig
from prestoadmin.util.json_tranform import as_key, purge
from prestoadmin.yarn_slider.config import SLIDER_CONFIG_DIR, SLIDER_USER, \
    SLIDER_PKG_NAME, JAVA_HOME, PRESTO_PACKAGE


class SliderJsonConfig(RequireableConfig):
    def __init__(self, config_path, default_config_path, transformations):
        super(SliderJsonConfig, self).__init__()
        self.json_from_default = JsonFileFromDefaultFile(
            config_path, default_config_path, transformations)
        self.present_file_config = PresentFileConfig(config_path)

    def get_config_path(self):
        return self.present_file_config.config_path

    @overrides
    def is_config_loaded(self):
        return self.present_file_config.is_config_loaded()

    @overrides
    def get_config(self):
        return self.json_from_default.get_config()


def _application_def(kpath, value):
    return os.path.join('.slider', 'package', SLIDER_PKG_NAME,
                        env.conf[PRESTO_PACKAGE])


def _env_java_home(kpath, value):
    return env.conf[JAVA_HOME]


def _prompt_data_dir(kpath, value):
    return prompt(
        "Enter a directory for presto to use for data:", default=value)

APP_CONFIG_TRANSFORMATIONS = {
    as_key('global', 'site.global.app_user'):
        lambda kpath, value: env.conf[SLIDER_USER],
    as_key('global', 'site.global.data_dir'): _prompt_data_dir,
    as_key('global', 'application.def'): _application_def,
    as_key('global', 'java.home'):
        lambda kpath, value: env.conf[JAVA_HOME],
    as_key('global', 'site.global.singlenode'):
        lambda kpath, value: False
}


class AppConfigJson(SliderJsonConfig):
    def __init__(self):
        super(AppConfigJson, self).__init__(
            os.path.join(SLIDER_CONFIG_DIR, 'appConfig.json'),
            os.path.join(SLIDER_CONFIG_DIR, 'appConfig-default.json'),
            APP_CONFIG_TRANSFORMATIONS)


def _prompt_worker_instances(kpath, value):
    return prompt(
        "Enter the number of worker nodes to create", default=value)


RESOURCES_TRANSFORMATIONS = {
    as_key('components', 'COORDINATOR', 'yarn.label.expression'): purge,
    as_key('components', 'WORKER', 'yarn.label.expression'): purge,
    as_key('components', 'WORKER', 'yarn.component.instances'):
        _prompt_worker_instances
}


class ResourcesJson(SliderJsonConfig):
    def __init__(self):
        super(ResourcesJson, self).__init__(
            os.path.join(SLIDER_CONFIG_DIR, 'resources.json'),
            os.path.join(SLIDER_CONFIG_DIR, 'resources-default.json'),
            RESOURCES_TRANSFORMATIONS)
