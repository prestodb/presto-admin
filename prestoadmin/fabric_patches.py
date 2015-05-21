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

"""Monkey patches needed to add logging to Fabric"""

import fabric.utils
import fabric.api
import fabric.operations
import fabric.tasks

from fabric.network import needs_host
import logging
from traceback import format_exc

_LOGGER = logging.getLogger(__name__)
old_warn = fabric.utils.warn
old_run = fabric.operations.run
old_sudo = fabric.operations.sudo


def warn(msg):
    if fabric.api.env.host:
        msg = '[' + fabric.api.env.host + '] ' + msg
    old_warn(msg)
    _LOGGER.warn(msg + '\n\n' + format_exc())


@needs_host
def run(command, shell=True, pty=True, combine_stderr=None, quiet=False,
        warn_only=False, stdout=None, stderr=None, timeout=None,
        shell_escape=None):
    out = old_run(command, shell=shell, pty=pty,
                  combine_stderr=combine_stderr, quiet=quiet,
                  warn_only=warn_only, stdout=stdout, stderr=stderr,
                  timeout=timeout, shell_escape=shell_escape)
    log_output(out)
    return out


@needs_host
def sudo(command, shell=True, pty=True, combine_stderr=None, user=None,
         quiet=False, warn_only=False, stdout=None, stderr=None, group=None,
         timeout=None, shell_escape=None):

    out = old_sudo(command, shell=shell, pty=pty,
                   combine_stderr=combine_stderr, user=user, quiet=quiet,
                   warn_only=warn_only, stdout=stdout, stderr=stderr,
                   group=group, timeout=timeout, shell_escape=shell_escape)
    log_output(out)
    return out


def log_output(out):
    _LOGGER.info('\nCOMMAND: ' + out.command + '\nFULL COMMAND: ' +
                 out.real_command + '\nSTDOUT: ' + out + '\nSTDERR: '
                 + out.stderr)


def _is_network_error_ignored():
    return False

# Need to monkey patch Fabric's warn method in order to print out
# all exceptions seen to the logs.
fabric.utils.warn = warn
fabric.api.warn = warn

# Also need to monkey patch run and sudo so that the stdout and stderr
# also go to the logs.
fabric.operations.run = run
fabric.api.run = run
fabric.operations.sudo = sudo
fabric.api.sudo = sudo
fabric.tasks._is_network_error_ignored = _is_network_error_ignored
