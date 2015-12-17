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
#

"""Presto-Admin tool for deploying and managing Presto clusters"""

__version__ = '1.2'  # Make sure to update setup.py too


import os
import sys

main_dir = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))

from fabric.api import env, abort

import fabric_patches
import mode

from prestoadmin.mode import get_mode, for_mode, MODE_STANDALONE, MODE_SLIDER
from prestoadmin.util.exception import ConfigFileNotFoundError, \
    ConfigurationError


__all__ = ['fabric_patches', 'mode']

cfg_mode = None
try:
    cfg_mode = get_mode()
except ConfigFileNotFoundError as e:
    print >>sys.stderr, \
        "Select a mode using the subcommand 'mode select <mode>'"
except ConfigurationError as e:
    print >>sys.stderr, e.message


ADDITIONAL_TASK_MODULES = {
    MODE_SLIDER: [('yarn_slider.server', 'server'),
                  ('yarn_slider.slider', 'slider')],
    MODE_STANDALONE: ['topology',
                      ('configure_cmds', 'configuration'),
                      'server',
                      'connector',
                      'package',
                      'collect',
                      'script',
                      'plugin']}


if cfg_mode is not None:
    atms = for_mode(cfg_mode, ADDITIONAL_TASK_MODULES)
    for atm in atms:
        try:
            module, subcommand_name = atm
        except ValueError:
            module = atm
            subcommand_name = atm

        __all__.append(subcommand_name)

        components = module.split('.')

        if len(components) == 1:
            # The simple case...
            # import <module> as <subcommand_name>
            globals()[subcommand_name] = __import__(module, globals())
        else:
            # The complicated case:
            # import foo.bar doesn't actually import foo.bar; it imports foo.
            # This is why, for example, you can't to the following:

            # >>> import os.path
            # >>> path.join('foo', 'bar', 'baz', 'zot')
            #
            # Doing the equivalent of import yarn_slider.slider as slider
            # results in the global slider variable being assigned to the
            # yarn_slider module, which is NOT what we want.
            # Instead, we need to recursively traverse the submodules until we
            # get to the one we're interested in.
            submodule = __import__(module, globals())
            for c in components[1:]:
                submodule = submodule.__dict__[c]
            globals()[subcommand_name] = submodule


env.roledefs = {
    'coordinator': [],
    'worker': [],
    'all': []
}
