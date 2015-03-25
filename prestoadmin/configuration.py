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


def get_conf_from_file(path):
    conf_file = open(path, 'r')
    return json.load(conf_file)


def write(conf, path):
    conf_directory = os.path.dirname(path)
    if not os.path.exists(conf_directory):
        os.makedirs(conf_directory)

    f = open(path, "w")
    json.dump(conf, f, indent=4, separators=(',', ':'))
    f.close


def fill_defaults(conf, defaults):
    for k, v in defaults.iteritems():
        conf.setdefault(k, v)
