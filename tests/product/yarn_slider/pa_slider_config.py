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
Module to manipulate presto-admin's slider configuration.
"""

import os
import json

from tests.product import determine_jdk_directory
from prestoadmin.yarn_slider.config import SLIDER_CONFIG_PATH, HOST, DIR, \
    SLIDER_USER, ADMIN_USER, HADOOP_CONF, SSH_PORT, \
    JAVA_HOME, APPNAME


def get_config(cluster, override=None):
    jdk_dir = os.path.join('/usr/java', determine_jdk_directory(cluster))
    conf = {
        DIR: '/opt/slider',
        ADMIN_USER: 'root',
        HADOOP_CONF: '/etc/hadoop/conf',
        SSH_PORT: 22,
        SLIDER_USER: 'yarn',
        HOST: 'master',
        JAVA_HOME: jdk_dir,
        APPNAME: 'presto'
    }

    if override:
        conf.update(override)
    return conf


def pick_config(conf, pick):
    result = {}
    for key in conf:
        value = conf[key]
        if isinstance(value, tuple):
            cluster_value, docker_value = value
            result[key] = pick(cluster_value, docker_value)
        else:
            result[key] = value

    return result


def cluster_config(conf):
    def pick(cluster_value, docker_value):
        return cluster_value
    return pick_config(conf, pick)


def docker_config(conf):
    def pick(cluster_value, docker_value):
        return docker_value
    return pick_config(conf, pick)


def upload_config(cluster, conf):
    cluster.write_content_to_host(
        json.dumps(cluster_config(conf)), SLIDER_CONFIG_PATH, cluster.get_master())
    return conf
