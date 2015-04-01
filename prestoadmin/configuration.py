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


class ConfigurationError(Exception):
    pass


def get_conf_from_file(path):
    try:
        conf_file = open(path, 'r')
    except IOError:
        raise ConfigurationError("Missing configuration file at " + repr(path))
    try:
        return json.load(conf_file)
    except ValueError as e:
        raise ConfigurationError(e)


def json_to_string(conf):
    return json.dumps(conf, indent=4, separators=(',', ':'))


def write(output, path):
    conf_directory = os.path.dirname(path)
    if not os.path.exists(conf_directory):
        os.makedirs(conf_directory)

    f = open(path, 'w')
    f.write(output)
    f.close


def fill_defaults(conf, defaults):
    try:
        default_items = defaults.iteritems()
    except AttributeError:
        return

    for k, v in default_items:
        conf.setdefault(k, v)
        fill_defaults(conf[k], v)
