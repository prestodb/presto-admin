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
An extension to optparse for presto-admin which logs user parsing errors.
"""

import logging
from optparse import OptionParser
import sys


_LOGGER = logging.getLogger(__name__)


class LoggingOptionParser(OptionParser):
    """
    An extension to optparse which logs exceptions via the logging
    module in addition to writing the out to stderr.

    If used with HiddenOptionGroup, print_extended_help disables the
    suppress_help attribute of HiddenOptionGroup so as to print out
    extended helptext.
    """

    def exit(self, status=0, msg=None):
        _LOGGER.debug("Exiting option parser!")
        if msg:
            sys.stderr.write(msg)
            _LOGGER.error(msg)
        sys.exit(status)

    def print_extended_help(self, filename=None):
        old_suppress_help = {}
        for group in self.option_groups:
            try:
                old_suppress_help[group] = group.suppress_help
                group.suppress_help = False
            except AttributeError as e:
                old_suppress_help[group] = None
                _LOGGER.debug("Option group does not have option to "
                              "suppress help; exception is " + e.message)
        self.print_help(file=filename)

        for group in self.option_groups:
            # Restore the suppressed help when applicable
            if old_suppress_help[group]:
                group.suppress_help = True

    def format_epilog(self, formatter):
        """
        The default format_epilog strips the newlines (using textwrap),
        so we override format_epilog here to use its own epilog
        """
        if not self.epilog:
            self.epilog = ""
        return self.epilog
