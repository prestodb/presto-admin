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
import prestoadmin

REQUIRED_FILES = ["node.properties", "jvm.config", "config.properties"]
TMP_CONF_DIR = prestoadmin.main_dir + "/tmp/presto-conf"


class ConfigurationError(Exception):
    pass


class ConfigFileNotFoundError(ConfigurationError):
    pass


def get_conf_from_file(path):
    try:
        with open(path, 'r') as conf_file:
            return json.load(conf_file)
    except IOError:
        raise ConfigFileNotFoundError("Missing configuration file at " +
                                      repr(path))
    except ValueError as e:
        raise ConfigurationError(e)


def json_to_string(conf):
    return json.dumps(conf, indent=4, separators=(',', ':'))


def write(output, path):
    conf_directory = os.path.dirname(path)
    if not os.path.exists(conf_directory):
        os.makedirs(conf_directory)

    with open(path, 'w') as f:
        f.write(output)


def fill_defaults(conf, defaults):
    try:
        default_items = defaults.iteritems()
    except AttributeError:
        return

    for k, v in default_items:
        conf.setdefault(k, v)
        fill_defaults(conf[k], v)


def validate_presto_conf(conf):
    for required in REQUIRED_FILES:
        if required not in conf:
            raise ConfigurationError("Missing configuration for required "
                                     "file: " + required)

    validate_presto_types(conf)
    return conf


def validate_presto_types(conf):
    expect_object_msg = "%s must be an object with key-value property pairs"
    try:
        if not isinstance(conf["node.properties"], dict):
            raise ConfigurationError(expect_object_msg % "node.properties")
    except KeyError:
        pass
    try:
        if not isinstance(conf["jvm.config"], list):
            raise ConfigurationError("jvm.config must contain a json "
                                     "array of jvm arguments "
                                     "([arg1, arg2, arg3])")
    except KeyError:
        pass
    try:
        if not isinstance(conf["config.properties"], dict):
            raise ConfigurationError(expect_object_msg % "config.properties")
    except KeyError:
        pass
    return conf
