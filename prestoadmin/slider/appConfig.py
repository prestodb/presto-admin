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
import os

from fabric.state import env
from fabric.utils import abort
from fabric.operations import prompt

from prestoadmin.config import get_conf_from_json_file

from prestoadmin.slider.server import SLIDER_PKG_DEFAULT_DEST as CONF_DIR
from prestoadmin.slider.config import SLIDER_USER, JAVA_HOME

CONF_FILENAME = 'appConfig.json'
DEFAULT_CONF_FILENAME = 'appConfig-default.json'
CONF_FILE = os.path.join (CONF_DIR, CONF_FILENAME)
DEFAULT_CONF_FILE = os.path.join (CONF_DIR, DEFAULT_CONF_FILENAME)


def get_appConfig():
    if os.path.exists(CONF_FILE):
        return get_conf_from_json_file(CONF_FILE)
    elif os.path.exists(DEFAULT_CONF_FILE):
        default_conf = get_conf_from_json_file(DEFAULT_CONF_FILE)
        return copy_default(default_conf, [])
    else:
        abort('Config file %s missing. Default config file %s also missing. ' +
              'Could not generate config' % (CONF_FILE, DEFAULT_CONF_FILE))


def get_app_user(kpath, value):
    return env.conf[SLIDER_USER]


def get_app_user(kpath, value):
    return env.conf[JAVA_HOME]


def purge(kpath, value):
    return None


def get_data_dir(kpath, value):
    prompt("Enter a directory for presto to use for data:", default=value)

def _keyify(path):
    return '/'.join(path)


APP_CONFIG_TRANSFORMS = {
    _keyify(['global', 'site.global.app_user']): get_app_user,
    _keyify(['global', 'site.global.presto_server_port']): purge,
    _keyify(['global', 'site.global.data_dir']): get_data_dir
}


def get_transform(kpath):
    return APP_CONFIG_TRANSFORMS.get(_keyify(kpath))


def copy_default(default, path):
    if type(default) is dict:
        result = {}
        for k in default:
            kpath = path[:]
            kpath.append(k)
            transform = get_transform(kpath)
            print "%s: %s" % (k, default[k])
            if transform:
                tvalue = transform(kpath, default[k])
                if tvalue is None:
                    result[k] = tvalue
            else:
                result[k] = copy_default(default[k], kpath)
        return result
    elif type(default) is list:
        result = []
        for i in default:
            result.append(copy_default(i))
        return result
    else:
        return default
