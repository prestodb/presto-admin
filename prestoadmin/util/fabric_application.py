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
Logic for starting and stopping Fabric applications.
"""

from fabric.network import disconnect_all
from prestoadmin.util.application import Application

import logging
import sys


# Normally this would use the logger for __name__, however, this is
# effectively the "root" logger for the application.  If this code
# were running directly in the executable script __name__ would be
# set to '__main__', so we emulate that same behavior here.  This should
# resolve to the same logger that will be used by the entry point script.
logger = logging.getLogger('__main__')


class FabricApplication(Application):
    """
    A Presto Fabric application entry point.  Provides logging and exception
    handling features.  Additionally cleans up Fabric network connections
    before exiting.
    """

    def _exit_cleanup_hook(self):
        """
        Disconnect all Fabric connections in addition to shutting down the
        logging.
        """
        disconnect_all()
        Application._exit_cleanup_hook(self)

    def _handle_error(self):
        """
        Handle KeyboardInterrupt in a special way: don't indicate
        that it's an error.

        Returns:
            Nothing
        """
        self._log_exception()
        if isinstance(self.exception, KeyboardInterrupt):
            print >> sys.stderr, "Stopped."
            sys.exit(0)
        else:
            Application._handle_error(self)
