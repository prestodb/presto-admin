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

from prestoadmin.util.constants import LOG_DIR_ENV_VARIABLE, CONFIG_DIR_ENV_VARIABLE, DEFAULT_LOCAL_CONF_DIR, \
    TOPOLOGY_CONFIG_FILE, COORDINATOR_DIR_NAME, WORKERS_DIR_NAME, CATALOG_DIR_NAME


def get_config_directory():
    config_directory = os.environ.get(CONFIG_DIR_ENV_VARIABLE)
    if not config_directory:
        config_directory = DEFAULT_LOCAL_CONF_DIR
    return config_directory


def get_log_directory():
    config_directory = os.environ.get(LOG_DIR_ENV_VARIABLE)
    if not config_directory:
        config_directory = os.path.join(get_config_directory(), 'log')
    return config_directory


def get_topology_path():
    return os.path.join(get_config_directory(), TOPOLOGY_CONFIG_FILE)


def get_coordinator_directory():
    return os.path.join(get_config_directory(), COORDINATOR_DIR_NAME)


def get_workers_directory():
    return os.path.join(get_config_directory(), WORKERS_DIR_NAME)


def get_catalog_directory():
    return os.path.join(get_config_directory(), CATALOG_DIR_NAME)
