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

import config
from prestoadmin.util import constants
import prestoadmin.util.fabricapi as util
from fabric.api import env


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
    return config.get_presto_conf(constants.WORKERS_DIR)


def build_defaults():
    conf = copy.deepcopy(DEFAULT_PROPERTIES)
    coordinator = util.get_coordinator_role()[0]
    conf["config.properties"]["discovery.uri"] = "http://" + coordinator \
                                                 + ":8080"
    return conf


def islocalhost(hostname):
    return hostname in ["localhost", "127.0.0.1", "::1"]


def validate(conf):
    config.validate_presto_conf(conf)
    if conf["config.properties"]["coordinator"] != "false":
        raise config.ConfigurationError("Coordinator must be false in the "
                                        "worker's config.properties")
    uri = urlparse.urlparse(conf["config.properties"]["discovery.uri"])
    if islocalhost(uri.hostname) and len(env.roledefs['all']) > 1:
            raise config.ConfigurationError(
                "discovery.uri should not be local host in a "
                "multi-node cluster, but found " + urlparse.urlunparse(uri) +
                ".  You may have encountered this error by "
                "choosing a coordinator that is localhost and a worker that "
                "is not.  The default discovery-uri is "
                "http://<coordinator>:8080")
    return conf
