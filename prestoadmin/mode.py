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
Module for handling presto-admin mode-related functionality.
"""

import os

from fabric.api import abort, task
from fabric.decorators import runs_once

from prestoadmin import config
from prestoadmin.util.exception import ConfigurationError, \
    ConfigFileNotFoundError
from prestoadmin.util.local_config_util import get_config_directory

MODE_CONF_PATH = os.path.join(get_config_directory(), 'mode.json')
MODE_KEY = 'mode'

MODE_SLIDER = 'yarn_slider'
MODE_STANDALONE = 'standalone'

VALID_MODES = [MODE_SLIDER, MODE_STANDALONE]


def _load_mode_config():
    return config.get_conf_from_json_file(MODE_CONF_PATH)


def _store_mode_config(mode_config):
    config.write(config.json_to_string(mode_config), MODE_CONF_PATH)


def get_mode(validate=True):
    mode_config = _load_mode_config()
    mode = mode_config.get(MODE_KEY)

    if validate and mode is None:
        raise ConfigurationError(
            'Required key %s not found in configuration file %s' % (
                MODE_KEY, MODE_CONF_PATH))

    if validate and not validate_mode(mode):
        raise ConfigurationError(
            'Invalid mode %s in configuration file %s. Valid modes are %s' % (
                mode, MODE_CONF_PATH, ' '.join(VALID_MODES)))

    return mode


def validate_mode(mode):
    return mode in VALID_MODES


def for_mode(mode, mode_map):
    if sorted(mode_map.keys()) != sorted(VALID_MODES):
        raise Exception(
            'keys in for_nodes\n%s\ndo not match VALID_MODES\n%s' % (
                mode_map.keys(), VALID_MODES))
    return mode_map[mode]


@task
@runs_once
def select(new_mode):
    """
    Change the mode.
    """
    if not validate_mode(new_mode):
        abort('Invalid mode selection %s. Valid modes are %s' % (
            new_mode, ' '.join(VALID_MODES)))

    mode_config = {}
    try:
        mode_config = _load_mode_config()
    except ConfigFileNotFoundError:
        pass

    mode_config[MODE_KEY] = new_mode
    _store_mode_config(mode_config)


@task
@runs_once
def get():
    """
    Display the current mode.
    """
    mode = None
    try:
        mode = get_mode(validate=False)
        print mode
    except ConfigFileNotFoundError:
        abort("Select a mode using the subcommand 'mode select <mode>'")


@task
@runs_once
def list():
    """
    List the supported modes.
    """
    print ' '.join(VALID_MODES)
