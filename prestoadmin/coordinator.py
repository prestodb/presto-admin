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
Module for the presto coordinator's configuration.
Loads and validates the coordinator.json file and creates the files needed
to deploy on the presto cluster
"""
import copy
import logging

from fabric.api import env

import config
from prestoadmin.util import constants
from prestoadmin.util.exception import ConfigurationError

DEFAULT_PROPERTIES = {"node.properties":
                      {"node.environment": "presto",
                       "node.data-dir": "/var/lib/presto/data",
                       "plugin.config-dir": "/etc/presto/catalog",
                       "plugin.dir": "/usr/lib/presto/lib/plugin"},
                      "jvm.config": ["-server",
                                     "-Xmx1G",
                                     "-XX:+UseConcMarkSweepGC",
                                     "-XX:+ExplicitGCInvokesConcurrent",
                                     "-XX:+CMSClassUnloadingEnabled",
                                     "-XX:+AggressiveOpts",
                                     "-XX:+HeapDumpOnOutOfMemoryError",
                                     "-XX:OnOutOfMemoryError=kill -9 %p",
                                     "-XX:ReservedCodeCacheSize=150M"],
                      "config.properties": {"coordinator": "true",
                                            "discovery-server.enabled": "true",
                                            "http-server.http.port": "8080",
                                            "task.max-memory": "1GB"}
                      }

_LOGGER = logging.getLogger(__name__)


def get_conf():
    conf = _get_conf()
    for name in config.REQUIRED_FILES:
        if name not in conf:
            _LOGGER.debug("Coordinator configuration for %s not found.  "
                          "Default configuration will be deployed", name)
    defaults = build_defaults()
    config.fill_defaults(conf, defaults)
    validate(conf)
    return conf


def _get_conf():
    return config.get_presto_conf(constants.COORDINATOR_DIR)


def build_defaults():
    conf = copy.deepcopy(DEFAULT_PROPERTIES)
    coordinator = env.roledefs['coordinator'][0]
    workers = env.roledefs['worker']
    if coordinator in workers:
        conf["config.properties"]["node-scheduler."
                                  "include-coordinator"] = "true"
    conf["config.properties"]["discovery.uri"] = "http://" + coordinator \
                                                 + ":8080"

    validate(conf)
    return conf


def validate(conf):
    config.validate_presto_conf(conf)
    if conf["config.properties"]["coordinator"] != "true":
        raise ConfigurationError("Coordinator cannot be false in the "
                                 "coordinator's config.properties.")
    return conf
