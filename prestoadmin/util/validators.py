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
Module for validating configuration information supplied by the user.
"""
import re
import socket

from fabric.context_managers import settings
from fabric.operations import run, sudo

from prestoadmin.util.exception import ConfigurationError


def validate_username(username):
    if not isinstance(username, basestring):
        raise ConfigurationError('Username must be of type string.')
    return username


def validate_port(port):
    try:
        port_int = int(port)
    except TypeError:
        raise ConfigurationError('Port must be of type string, but '
                                 'found ' + str(type(port)) + '.')
    except ValueError:
        raise ConfigurationError('Invalid port number ' + port +
                                 ': port must be a number between 1 and 65535')
    if not port_int > 0 or not port_int < 65535:
        raise ConfigurationError('Invalid port number ' + port +
                                 ': port must be a number between 1 and 65535')
    return port_int


def validate_host(host):
    try:
        socket.inet_pton(socket.AF_INET, host)
        return host
    except TypeError:
        raise ConfigurationError('Host must be of type string.  Found ' +
                                 str(type(host)) + '.')
    except socket.error:
        pass

    try:
        socket.inet_pton(socket.AF_INET6, host)
        return host
    except socket.error:
        pass

    if not is_valid_hostname(host):
        raise ConfigurationError(repr(host) + ' is not a valid '
                                 'ip address or host name.')
    return host


def is_valid_hostname(hostname):
    valid_name = '^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*' \
                 '([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$'
    return re.match(valid_name, hostname)


def validate_can_connect(user, host, port):
    with settings(host_string='%s@%s:%d' % (user, host, port), user=user):
        return run('exit 0').succeeded


def validate_can_sudo(sudo_user, conn_user, host, port):
    with settings(host_string='%s@%s:%d' % (conn_user, host, port),
                  warn_only=True):
        return sudo('exit 0', user=sudo_user).succeeded
