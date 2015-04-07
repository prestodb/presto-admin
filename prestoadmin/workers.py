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
Module for the presto worker`'s configuration.
Loads and validates the workers.json file and creates the files needed
to deploy on the presto cluster
"""

import configuration
import copy
import prestoadmin
import prestoadmin.util.fabricapi as util

CONFIG_PATH = prestoadmin.main_dir + "/resources/workers.json"
TMP_OUTPUT_DIR = configuration.TMP_CONF_DIR + "/workers"
DEFAULT_PROPERTIES = {"node.properties": {"node.environment": "presto",
                                          "node.data-dir": "/var/lib/presto"
                                                           "/data"},
                      "jvm.config": ["-server",
                                     "-Xmx1G",
                                     "-XX:+UseConcMarkSweepGC",
                                     "-XX:+ExplicitGCInvokesConcurrent",
                                     "-XX:+CMSClassUnloadingEnabled",
                                     "-XX:+AggressiveOpts",
                                     "-XX:+HeapDumpOnOutOfMemoryError",
                                     "-XX:OnOutOfMemoryError=kill -9 %p",
                                     "-XX:ReservedCodeCacheSize=150M"],
                      "config.properties": {"coordinator": "false",
                                            "http-server.http.port": "8080",
                                            "task.max-memory": "1GB"}
                      }


def get_conf():
    conf = configuration.validate_presto_types(_get_conf_from_file())
    defaults = build_defaults()
    configuration.fill_defaults(conf, defaults)
    validate(conf)
    return conf


def _get_conf_from_file():
    try:
        configuration.get_conf_from_file(CONFIG_PATH)
    except configuration.ConfigFileNotFoundError:
        return {}


def build_defaults():
    conf = copy.deepcopy(DEFAULT_PROPERTIES)
    coordinator = util.get_coordinator_role()[0]
    conf["config.properties"]["discovery.uri"] = "http://" + coordinator \
                                                 + ":8080"

    validate(conf)
    return conf


def validate(conf):
    configuration.validate_presto_conf(conf)
    if conf["config.properties"]["coordinator"] != "false":
        raise configuration.ConfigurationError("Coordinator must be false "
                                               "in the worker's "
                                               "config.properties")
    return conf
