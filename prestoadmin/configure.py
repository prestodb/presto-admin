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
Module with common functions for presto configuration
"""

import configuration as config
import prestoadmin

REQUIRED_FILES = ["node.properties", "jvm.config", "config.properties"]
TMP_CONF_DIR = prestoadmin.main_dir + "/tmp/presto-conf"


def validate(conf):
    for required in REQUIRED_FILES:
        if required not in conf:
            raise config.ConfigurationError("Missing configuration for "
                                            "required file: " + required)

    validate_types(conf)
    return conf


def validate_types(conf):
    expect_object_msg = "%s must be an object with key-value property pairs"
    try:
        if not isinstance(conf["node.properties"], dict):
            raise config.ConfigurationError(expect_object_msg %
                                            "node.properties")
    except KeyError:
        pass
    try:
        if not isinstance(conf["jvm.config"], list):
            raise config.ConfigurationError("jvm.config must contain a json "
                                            "array of jvm arguments "
                                            "([arg1, arg2, arg3])")
    except KeyError:
        pass
    try:
        if not isinstance(conf["config.properties"], dict):
            raise config.ConfigurationError(expect_object_msg %
                                            "config.properties")
    except KeyError:
        pass
    return conf


def write_conf_to_tmp(conf, conf_dir):
    for key, value in conf.iteritems():
        path = conf_dir + "/" + key
        config.write(output_format(value), path)


def output_format(conf):
    try:
        return dict_to_equal_format(conf)
    except AttributeError:
        pass
    try:
        return list_to_line_separated(conf)
    except TypeError:
        pass
    except AssertionError:
        pass
    return str(conf)


def dict_to_equal_format(conf):
    sorted_list = sorted(key_val_to_equal(conf.iteritems()))
    return list_to_line_separated(sorted_list)


def key_val_to_equal(items):
    return ["=".join(item) for item in items]


def list_to_line_separated(conf):
    assert not isinstance(conf, basestring)
    return "\n".join(conf)
