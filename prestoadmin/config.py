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
Module for reading, writing, and processing configuration files
"""
import json
import os
import logging
import errno
import re

from prestoadmin.util.exception import ConfigurationError,\
    ConfigFileNotFoundError

COMMENT_CHARS = ['!', '#']
_LOGGER = logging.getLogger(__name__)


def get_conf_from_json_file(path):
    try:
        with open(path, 'r') as conf_file:
            if os.path.getsize(conf_file.name) == 0:
                return {}
            return json.load(conf_file)
    except IOError:
        raise ConfigFileNotFoundError(
            config_path=path, message="Missing configuration file %s." %
            (repr(path)))
    except ValueError as e:
        raise ConfigurationError(e)


def get_conf_from_properties_file(path):
    with open(path, 'r') as conf_file:
        return get_conf_from_properties_data(conf_file)


def get_conf_from_properties_data(data):
    props = {}
    for line in data.read().splitlines():
        line = line.strip()
        if len(line) > 0 and line[0] not in COMMENT_CHARS:
            pair = split_to_pair(line)
            props[pair[0]] = pair[1]
    return props


def split_to_pair(line):
    split_line = re.split(r'\s*(?<!\\):\s*|\s*(?<!\\)=\s*|(?<!\\)\s+', line,
                          maxsplit=1)
    if len(split_line) != 2:
        raise ConfigurationError(
            line + " is not in the expected format: <property>=<value>, "
                   "<property>:<value> or <property> <value>")
    return tuple(split_line)


def get_conf_from_config_file(path):
    with open(path, 'r') as conf_file:
        settings = conf_file.read().splitlines()
        return settings


def json_to_string(conf):
    return json.dumps(conf, indent=4, separators=(',', ':'))


def write_conf_to_file(conf, path):
    # Note: this function expects conf to be flat
    # either a dict for .properties file or a list for .config
    ext = os.path.splitext(path)[1]
    if ext == ".properties":
        write_properties_file(conf, path)
    elif ext == ".config":
        write_config_file(conf, path)


def write_properties_file(conf, path):
    output = ''
    for key, value in conf.iteritems():
        output += '%s=%s\n' % (key, value)
    write(output, path)


def write_config_file(conf, path):
    output = '\n'.join(conf)
    write(output, path)


def write(output, path):
    conf_directory = os.path.dirname(path)
    try:
        os.makedirs(conf_directory)
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise

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
