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
Module for setting and validating the presto-admin config
"""
import pprint

from fabric.api import env, runs_once, task

from prestoadmin.standalone.config import StandaloneConfig
from prestoadmin.util.base_config import requires_config

import prestoadmin.util.fabricapi as util


@task
@runs_once
@requires_config(StandaloneConfig)
def show():
    """
    Shows the current topology configuration for the cluster (including the
    coordinators, workers, SSH port, and SSH username)
    """
    pprint.pprint(get_conf_from_fabric(), width=1)


def get_conf_from_fabric():
    return {'coordinator': util.get_coordinator_role()[0],
            'workers': util.get_worker_role(),
            'port': env.port,
            'username': env.user}
