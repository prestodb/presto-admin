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

    def exit(self, status=0, msg=None):
        _LOGGER.debug("Exiting option parser!")
        if msg:
            sys.stderr.write(msg)
            _LOGGER.exception(msg)
        sys.exit(status)
