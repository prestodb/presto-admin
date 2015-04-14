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
An option group for which you can hide the help text.
"""

import logging
from optparse import OptionGroup


_LOGGER = logging.getLogger(__name__)


class HiddenOptionGroup(OptionGroup):
    """
    Optparse allows you to suppress Options from the help text, but not
    groups. This class allows you to suppress the help of groups.
    """

    def __init__(self, parser, title, description=None, suppress_help=False):
        OptionGroup.__init__(self, parser, title, description)
        self.suppress_help = suppress_help

    def format_help(self, formatter):
        if not self.suppress_help:
            return OptionGroup.format_help(self, formatter)
        else:
            return ""
