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
Commands for running scripts on a cluster
"""
import logging
from fabric.operations import put, sudo
from fabric.decorators import task
from os import path

from prestoadmin.standalone.config import StandaloneConfig
from prestoadmin.util.base_config import requires_config

_LOGGER = logging.getLogger(__name__)
__all__ = ['run']


@task
@requires_config(StandaloneConfig)
def run(script, remote_dir='/tmp'):
    """
    Run an arbitrary script on all nodes in the cluster.

    Parameters:
        script - The path to the script
        remote_dir - Where to put the script on the cluster.  Default is /tmp.
    """
    script_name = path.basename(script)
    remote_path = path.join(remote_dir, script_name)
    put(script, remote_path)
    sudo('chmod u+x %s' % remote_path)
    sudo(remote_path)
    sudo('rm %s' % remote_path)
