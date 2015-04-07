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
from fabric.decorators import task, runs_once
from fabric.operations import sudo
from fabric.api import env

from prestoadmin.util.fabricapi import execute_fail_on_error


__all__ = ['start', 'stop', 'restart']

INIT_SCRIPTS = '/etc/init.d/presto'


def service(control=None):
    sudo(INIT_SCRIPTS + control, pty=False)


@task
@runs_once
def start():
    """
    Start the Presto server
    """
    execute_fail_on_error(service, ' start', roles=env.roles)


@task
@runs_once
def stop():
    """
    Stop the Presto server
    """
    execute_fail_on_error(service, ' stop', roles=env.roles)


@task
@runs_once
def restart():
    """
    Restart the Presto server
    """
    execute_fail_on_error(service, ' restart', roles=env.roles)
