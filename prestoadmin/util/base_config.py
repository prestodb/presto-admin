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
Module for common configuration stuff.
"""

import abc
import os

from functools import wraps

from fabric.context_managers import settings
from fabric.operations import prompt
from fabric.state import env

from prestoadmin import config
from prestoadmin.config import ConfigFileNotFoundError
from prestoadmin.util.exception import ConfigurationError


def set_env_hosts(hosts_list, hosts_source):
    if 'pa_hosts_source' in env and env.pa_hosts_source is not None:
        raise ConfigurationError(
            'We goofed! env.hosts was already set based on the configuration '
            'in %s, but we are trying to set it again based on the '
            'configuration in %s. Please file an issue on github and let us '
            'know what command you were trying to run.'
            % (env.pa_hosts_source, hosts_source))

    env.pa_hosts_source = hosts_source
    env.hosts = hosts_list


def sanitize_env_hosts_source():
    if 'pa_hosts_source' in env:
        del env['pa_hosts_source']


def get_env_hosts_source():
    if 'pa_hosts_source' in env:
        return env.pa_hosts_source
    else:
        return None


class SingleConfigItem(object):
    def __init__(self, key, prompt, default=None, validate=None):
        self.key = key
        self.prompt = prompt
        self.default = default
        self.validate = validate

    def prompt_user(self, conf):
        conf[self.key] = prompt(self.prompt,
                                default=conf.get(self.key, self.default),
                                validate=self.validate)

    def collect_prompts(self, l):
        l.append((self.prompt, self.key))


class MultiConfigItem(object):
    def __init__(self, items, validate, validate_keys,
                 validate_failed_text):
        self.items = items
        self.validate = validate
        self.validate_keys = validate_keys
        self.validate_failed_text = validate_failed_text

    def prompt_user(self, conf):
        while True:
            for item in self.items:
                item.prompt_user(conf)

            validate_args = [conf[k] for k in self.validate_keys]
            if self.validate(*validate_args):
                break
            print (self.validate_failed_text % self.validate_keys) % conf

    def collect_prompts(self, l):
        for item in self.items:
            item.collect_prompts(l)


def requires_config(*config_classes):
    def wrap(func):
        config_instances = [config_class() for config_class in config_classes]
        func.pa_config_callbacks = \
            [config_inst.get_config for config_inst in config_instances]

        @wraps(func)
        def wrapper(*args, **kwargs):
            for config_instance in config_instances:
                if not config_instance.is_config_loaded():
                    raise ConfigurationError('Required config not loaded at '
                                             'task execution time.')
            return func(*args, **kwargs)
        return wrapper
    return wrap


class RequireableConfig(object):
    '''
    RequirableConfig represents the minimum functionality a configuration needs
    to implement to be compatible with the @requires_config decorator.

    Subclasses must have additionally have a no-arguments constructor in order
    to be compatible with @requires_config.
    '''
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        super(RequireableConfig, self).__init__()

    @abc.abstractmethod
    def is_config_loaded(self):
        pass

    @abc.abstractmethod
    def get_config(self):
        pass


class PresentFileConfig(RequireableConfig):
    def __init__(self, config_path):
        super(PresentFileConfig, self).__init__()
        self.config_path = config_path

    def get_config(self):
        if not self.is_config_loaded():
            raise ConfigFileNotFoundError(
                message='The required configuration file %s is missing. Create '
                        'it and try again' % (self.config_path),
                        config_path=self.config_path)

    def is_config_loaded(self):
        return os.path.exists(self.config_path)


class FabricEnvConfig(RequireableConfig):
    '''
    FabricEnvConfig provides the common config functionality for loading
    configuration files for presto-admin and going through the interactive
    config process if a config file isn't present.

    Instances of classes that subclass FabricEnvConfig are intended to be used
    with the @requires_config decorator, which is responsible for adding an
    attribute to the task that tells main() how to load the configuration
    and subsequently for enforcing that the configuration has been loaded at
    the time the task is actually run.

    In order to be compatible with @requires_config, subclasses must define
    a no-arguments constructor.
    '''
    __metaclass__ = abc.ABCMeta

    def __init__(self, config_path, config_items):
        super(FabricEnvConfig, self).__init__()
        self.config_path = config_path
        self.config_items = config_items

    def read_conf(self):
        return config.get_conf_from_json_file(self.config_path)

    def write_conf(self, conf):
        config.write(config.json_to_string(conf), self.config_path)
        return self.config_path

    def get_conf_interactive(self):
        conf = {}
        for item in self.config_items:
            item.prompt_user(conf)
        return conf

    def get_config(self):
        with settings(parallel=False):
            if not self.is_config_loaded():
                conf = {}
                try:
                    conf = self.read_conf()
                except ConfigFileNotFoundError:
                    conf = self.get_conf_interactive()
                    self.write_conf(conf)

                self.set_env_from_conf(conf)
                self.set_config_loaded()
            return self.config_path

    @abc.abstractmethod
    def set_config_loaded(self):
        pass

    @abc.abstractmethod
    def set_env_from_conf(self, conf):
        pass
