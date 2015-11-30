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
module for tasks relating to presto plugins
"""
import logging
from fabric.decorators import task
from fabric.operations import sudo, put
import os
from fabric.api import env
from prestoadmin.standalone.config import StandaloneConfig
from prestoadmin.util.base_config import requires_config
from prestoadmin.util.constants import REMOTE_PLUGIN_DIR

__all__ = ['add_jar']
_LOGGER = logging.getLogger(__name__)


def write(local_path, remote_dir):
    sudo("mkdir -p " + remote_dir)
    put(local_path, remote_dir, use_sudo=True)


@task
@requires_config(StandaloneConfig)
def add_jar(local_path, plugin_name, plugin_dir=REMOTE_PLUGIN_DIR):
    """
    Deploy jar for the specified plugin to the plugin directory.

    Parameters:
        local_path - Local path to the jar to be deployed
        plugin_name - Name of the plugin subdirectory to deploy jars to
        plugin_dir - (Optional) The plugin directory.  If no directory is
                     given, '/usr/lib/presto/lib/plugin' is used by default.
    """
    _LOGGER.info('deploying jars on %s' % env.host)
    write(local_path, os.path.join(plugin_dir, plugin_name))
