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

import copy
import logging
import urlparse

import configuration
import prestoadmin
import prestoadmin.util.fabricapi as util
from fabric.api import env


CONFIG_PATH = prestoadmin.main_dir + "/resources/workers.json"
TMP_OUTPUT_DIR = configuration.TMP_CONF_DIR + "/workers"
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
                      "config.properties": {"coordinator": "false",
                                            "http-server.http.port": "8080",
                                            "task.max-memory": "1GB"}
                      }

_LOGGER = logging.getLogger(__name__)


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
        _LOGGER.debug("Coordinator configuration %s not found. Default "
                      "configuration will be deployed."
                      % CONFIG_PATH)
        return {}


def build_defaults():
    conf = copy.deepcopy(DEFAULT_PROPERTIES)
    coordinator = util.get_coordinator_role()[0]
    conf["config.properties"]["discovery.uri"] = "http://" + coordinator \
                                                 + ":8080"

    validate(conf)
    return conf


def islocalhost(hostname):
    return hostname in ["localhost", "127.0.0.1", "::1"]


def validate(conf):
    configuration.validate_presto_conf(conf)
    if conf["config.properties"]["coordinator"] != "false":
        raise configuration.ConfigurationError("Coordinator must be false "
                                               "in the worker's "
                                               "config.properties")
    uri = urlparse.urlparse(conf["config.properties"]["discovery.uri"])
    if islocalhost(uri.hostname) and len(env.roledefs['all']) > 1:
            raise configuration.ConfigurationError(
                "discovery.uri should not be local host in a "
                "multi-node cluster, but found " + urlparse.urlunparse(uri) +
                "You may have encountered this error by "
                "choosing a coordinator that is localhost and a worker that "
                "is not.  The default discovery-uri is "
                "http://coordinator:8080")
    return conf
