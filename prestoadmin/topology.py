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
This module contains the methods for setting and validating the
presto-admin configuration
"""

import configuration
from fabric.api import task
import os
import pprint
import re
import socket

__all__ = ["show"]

CONFIG_PATH = os.path.dirname(__file__) + "/../resources/config.json"
PRESTO_ADMIN_PROPERTIES = ["username", "port", "coordinator", "workers"]
DEFAULT_PROPERTIES = {"username": "root",
                      "port": "22",
                      "coordinator": "localhost",
                      "workers": ["localhost"]}


class ConfigurationError(Exception):
    pass


def write(conf):
    configuration.write(conf, CONFIG_PATH)


@task
def show():
    pprint.pprint(get_conf(), width=1)


def get_conf():
    conf = _get_conf_from_file()
    validate(conf)
    configuration.fill_defaults(conf, DEFAULT_PROPERTIES)
    return conf


def _get_conf_from_file():
    return configuration.get_conf_from_file(CONFIG_PATH)


def set_conf_interactive():
    write(build_conf_interactive())


def build_conf_interactive():
    conf = {}
    conf["username"] = read_in_username(prompt_for_username)
    conf["port"] = read_in_port(prompt_for_port)
    conf["coordinator"] = read_in_coordinator(prompt_for_coordinator)
    conf["workers"] = read_in_workers(prompt_for_workers)
    return conf


def read_in_username(input_func):
    prop = read_in_property(input_func, validate_username)
    return prop if prop else DEFAULT_PROPERTIES["username"]


def read_in_port(input_func):
    prop = read_in_property(input_func, validate_port)
    return prop if prop else DEFAULT_PROPERTIES["port"]


def read_in_coordinator(input_func):
    prop = read_in_property(input_func, validate_coordinator)
    return prop if prop else DEFAULT_PROPERTIES["coordinator"]


def read_in_workers(input_func):
    prop = read_in_property(input_func, validate_workers)
    return prop if prop else DEFAULT_PROPERTIES["workers"]


def prompt_for_username():
    return raw_input("Enter user name for SSH connection to all nodes "
                     "(Default: root): ")


def prompt_for_port():
    return raw_input("Enter port number for SSH connections to all nodes "
                     "(Default: 22): ")


def prompt_for_coordinator():
    return raw_input("Enter host name or IP address for coordinator node "
                     "(Default: localhost): ")


def prompt_for_workers():
    workers = raw_input("Enter host names or IP addresses for worker nodes"
                        "(i.e. node1 node2) (Default: localhost): ")
    return workers.split()


def read_in_property(input_func, validate_func):
    while True:
        prop = input_func()
        try:
            if prop:
                validate_func(prop)
            return prop
        except ConfigurationError as e:
            print e


def validate(conf):
    for key in conf.keys():
        if key not in PRESTO_ADMIN_PROPERTIES:
            raise ConfigurationError("Invalid property: " + key)

    try:
        username = conf["username"]
    except KeyError:
        pass
    else:
        validate_username(username)

    try:
        coordinator = conf["coordinator"]
    except KeyError:
        pass
    else:
        validate_coordinator(coordinator)

    try:
        workers = conf["workers"]
    except KeyError:
        pass
    else:
        validate_workers(workers)

    try:
        ssh_port = conf["ssh-port"]
    except KeyError:
        pass
    else:
        validate_port(ssh_port)


def validate_username(username):
    if not isinstance(username, basestring):
        raise ConfigurationError("username must be of type string")


def validate_coordinator(coordinator):
    validate_host(coordinator)


def validate_workers(workers):
    if not isinstance(workers, list):
        raise ConfigurationError("Workers must be of type list.  Found "
                                 + str(type(workers)))

    if len(workers) < 1:
        raise ConfigurationError("Must specify at least one worker")

    for worker in workers:
        validate_host(worker)


def validate_port(port_string):
    try:
        port = int(port_string)
    except TypeError:
        raise ConfigurationError("Port must be of type string, but found "
                                 + str(type(port_string)))
    except ValueError:
        raise ConfigurationError("Invalid value " + port_string + ": "
                                 "port must be a number between 1 and 65535")
    if not port > 0 or not port < 65535:
        raise ConfigurationError("Invalid port number " + str(port) +
                                 ": port must be between 1 and 65535")


def validate_host(host):
    try:
        socket.inet_pton(socket.AF_INET, host)
        return
    except TypeError:
        raise ConfigurationError("Host must be of type string.  Found "
                                 + str(type(host)))
    except socket.error:
        pass

    try:
        socket.inet_pton(socket.AF_INET6, host)
        return
    except socket.error:
        pass

    if not is_valid_hostname(host):
        raise ConfigurationError(repr(host) + " is not a valid "
                                 "ip address or host name")


def is_valid_hostname(hostname):
    valid_name = "^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*"\
                 "([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$"
    return re.match(valid_name, hostname)


def get_coordinator():
    conf = get_conf()
    return conf["coordinator"]


def get_workers():
    conf = get_conf()
    return conf["workers"]


def get_username():
    conf = get_conf()
    return conf["username"]


def get_port():
    conf = get_conf()
    return conf["port"]
