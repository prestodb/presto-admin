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
This module defines error types relevant to the Presto
administrative suite.
"""
import re

import sys
import traceback


# Beware the nuances of pickling Exceptions:
# http://bugs.python.org/issue1692335
class ExceptionWithCause(Exception):

    def __init__(self, message=''):
        self.inner_exception = None

        causing_exception = sys.exc_info()[1]
        if causing_exception:
            self.inner_exception = traceback.format_exc() + \
                ExceptionWithCause.get_cause_if_supported(causing_exception)

        super(ExceptionWithCause, self).__init__(message)

    @staticmethod
    def get_cause_if_supported(exception):
        try:
            inner = exception.inner_exception
        except AttributeError:
            inner = None

        if inner:
            return '\nCaused by:\n{tb}'.format(
                tb=inner
            )
        else:
            return ''


class InvalidArgumentError(ExceptionWithCause):
    pass


class ConfigurationError(ExceptionWithCause):
    pass


class ConfigFileNotFoundError(ConfigurationError):
    def __init__(self, message='', config_path=''):
        super(ConfigFileNotFoundError, self).__init__(message)
        self.config_path = config_path


def is_arguments_error(exception):
    return isinstance(exception, TypeError) and \
        re.match(r'.+\(\) takes (at most \d+|no|exactly \d+|at least \d+) '
                 r'arguments? \(\d+ given\)', exception.message)
