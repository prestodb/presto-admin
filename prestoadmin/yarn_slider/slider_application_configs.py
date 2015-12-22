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

import ast
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
        lambda kpath, value: False,
    as_key('global', 'site.global.catalog'):
        lambda kpath, value: "{'tpch': ['connector.name=tpch']}"
}


class AppConfigJson(SliderJsonConfig):
    def __init__(self):
        super(AppConfigJson, self).__init__(
            os.path.join(SLIDER_CONFIG_DIR, 'appConfig.json'),
            os.path.join(SLIDER_CONFIG_DIR, 'appConfig-default.json'),
            APP_CONFIG_TRANSFORMATIONS)
        self.config = None

    def get_config(self):
        if not self.config:
            self.config = super(AppConfigJson, self).get_config()
        return self.config

    def get_jvm_args(self):
        jvm_args = self.get_config()['global']['site.global.jvm_args']
        if jvm_args is None:
            return jvm_args
        return ast.literal_eval(jvm_args)

    def get_data_dir(self):
        return self.get_config()['global']['site.global.data_dir']

    def get_user(self):
        return self.get_config()['global']['site.global.app_user']

    def get_group(self):
        return self.get_config()['global']['site.global.user_group']

    def get_presto_server_port(self):
        return self.get_config()['global']['site.global.presto_server_port']


def _prompt_worker_instances(kpath, value):
    return prompt(
        "Enter the number of worker nodes to create", default=value)

ONE_KiB = 2 ** 10
ONE_MiB = 2 ** 20
ONE_GiB = 2 ** 30

# Allowed values per the JRockit documentation [0] because I couldn't find the
# Hotspot JVM documentation and got tired of looking. Presumably they're the
# same as JRockit was meant to be drop-in compatible with the Sun JRE before
# Oracle bought Sun [1].
# IEC rather than metric multiples because good luck finding any documenation
# on which Java actually uses, and err on the side of generosity.
# [0] http://docs.oracle.com/cd/E13150_01/jrockit_jvm/jrockit/jrdocs/refman/optionX.html#wp999528 # noqa
# [1] https://en.wikipedia.org/wiki/JRockit
UNIT_CHAR_LOOKUP = {
    'k': ONE_KiB, 'K': ONE_KiB,
    'm': ONE_MiB, 'M': ONE_MiB,
    'g': ONE_GiB, 'G': ONE_GiB,
}


def _get_max_heap(jvm_args):
    size = None
    for arg in jvm_args:
        if arg.startswith('-Xmx'):
            size = arg
            break

    if size is None:
        return None

    size = size[4:]
    suffix = size[-1]
    size = int(size[:-1])
    return size * UNIT_CHAR_LOOKUP.get(suffix, 1)


def _pad_presto_heap_for_yarn(kpath, value):
    padding = 512 * ONE_MiB
    appConfig = AppConfigJson()
    jvm_args = appConfig.get_jvm_args()
    max_heap = _get_max_heap(jvm_args)
    if max_heap is None:
        max_heap = 0
    return str((max_heap + padding) / ONE_MiB)


RESOURCES_TRANSFORMATIONS = {
    as_key('components', 'COORDINATOR', 'yarn.label.expression'): purge,
    as_key('components', 'WORKER', 'yarn.label.expression'): purge,
    as_key('components', 'WORKER', 'yarn.component.instances'):
        _prompt_worker_instances,
    as_key('components', 'COORDINATOR', 'yarn.memory'):
        _pad_presto_heap_for_yarn,
    as_key('components', 'WORKER', 'yarn.memory'):
        _pad_presto_heap_for_yarn
}


class ResourcesJson(SliderJsonConfig):
    def __init__(self):
        super(ResourcesJson, self).__init__(
            os.path.join(SLIDER_CONFIG_DIR, 'resources.json'),
            os.path.join(SLIDER_CONFIG_DIR, 'resources-default.json'),
            RESOURCES_TRANSFORMATIONS)
