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
Module to add extensions and helpers for fabric api methods
"""
from fabric.api import env
from fabric.tasks import execute


def execute_fail_on_error(callable, *args, **kwargs):
    """
    Wrapper around fabric.api.execute which verifies if executed command
    succeeded for each host and throws exception otherwise. Standard fabric
    execute command would not throw exception if execution for one of hosts
    fails in parallel mode.
    """
    execute_result = execute(callable, *args, **kwargs)
    for host in execute_result:
        if (execute_result[host] is not None and
                isinstance(execute_result[host], Exception)):
            raise Exception("command failed for some nodes; result=%s"
                            % execute_result)


def get_host_list():
    return [host for host in env.hosts if host not in env.exclude_hosts]


def get_coordinator_role():
    return env.roledefs['coordinator']


def get_worker_role():
    return env.roledefs['worker']
