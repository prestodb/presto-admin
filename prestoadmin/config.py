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
import logging

REQUIRED_FILES = ["node.properties", "jvm.config", "config.properties"]
PRESTO_FILES = ["node.properties", "jvm.config", "config.properties",
                "log.properties"]
_LOGGER = logging.getLogger(__name__)


class ConfigurationError(Exception):
    pass


class ConfigFileNotFoundError(ConfigurationError):
    pass


def get_conf_from_json_file(path):
    try:
        with open(path, 'r') as conf_file:
            if os.path.getsize(conf_file.name) == 0:
                return {}
            return json.load(conf_file)
    except IOError:
        raise ConfigFileNotFoundError("Missing configuration file at " +
                                      repr(path))
    except ValueError as e:
        raise ConfigurationError(e)


def get_conf_from_properties_file(path):
    with open(path, 'r') as conf_file:
        props = {}
        for line in conf_file.read().splitlines():
            line = line.strip()
            if len(line) > 0:
                pair = split_to_pair(line)
                props[pair[0]] = pair[1]
        return props


def split_to_pair(line):
    split_line = line.split("=", 1)
    if len(split_line) != 2:
        raise ConfigurationError(
            line + " is not in the expected format: <property>=<value>")
    return tuple(split_line)


def get_conf_from_config_file(path):
    with open(path, 'r') as conf_file:
        settings = conf_file.read().splitlines()
        return settings


def get_presto_conf(conf_dir):
    if os.path.isdir(conf_dir):
        file_list = [name for name in os.listdir(conf_dir) if
                     name in PRESTO_FILES]
    else:
        _LOGGER.debug("No directory " + conf_dir)
        file_list = []

    conf = {}
    for filename in file_list:
        ext = os.path.splitext(filename)[1]
        file_path = os.path.join(conf_dir, filename)
        if ext == ".properties":
            conf[filename] = get_conf_from_properties_file(file_path)
        elif ext == ".config":
            conf[filename] = get_conf_from_config_file(file_path)
    return conf


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

    expect_object_msg = "%s must be an object with key-value property pairs"
    if not isinstance(conf["node.properties"], dict):
        raise ConfigurationError(expect_object_msg % "node.properties")

    if not isinstance(conf["jvm.config"], list):
        raise ConfigurationError("jvm.config must contain a json array of jvm "
                                 "arguments ([arg1, arg2, arg3])")

    if not isinstance(conf["config.properties"], dict):
        raise ConfigurationError(expect_object_msg % "config.properties")
    return conf
