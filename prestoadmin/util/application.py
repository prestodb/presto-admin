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

"""Logic at the application level for logging and exception handling"""

import functools
import logging
import logging.config
import os
import sys
import traceback

import __main__ as main

from prestoadmin import __version__
from prestoadmin.util import constants
from prestoadmin.util import filesystem
from prestoadmin.util.exception import ExceptionWithCause
from prestoadmin.util.local_config_util import get_log_directory

# Normally this would use the logger for __name__, however, this is
# effectively the "root" logger for the application.  If this code
# were running directly in the executable script __name__ would be
# set to '__main__', so we emulate that same behavior here.  This should
# resolve to the same logger that will be used by the entry point script.
logger = logging.getLogger('__main__')


class Application(object):
    """
    A generic application entry point.  Provides logging and exception handling
    features.  This class is expected to be used as a base class for various
    applications.

    Parameters:
        name - human readable name for the application
        version - the version of the application, as a string
        log_file_path - optional name of the log file including whatever
        extension you may want to use.  For example, 'foo.log' would create
        a file called 'foo.log' in the default presto-admin logging directory
        tree.
    """

    def __init__(self, name, version=None, log_file_path=None):
        self.name = str(name)
        self.__log_file_path = log_file_path or (self.name + '.log')
        if not os.path.isabs(self.__log_file_path):
            self.__log_file_path = os.path.join(
                get_log_directory(),
                self.__log_file_path
            )

        self.version = version or __version__

    def __enter__(self):
        self.__configure_logging()
        return self

    def __configure_logging(self):
        try:
            for maybe_file_path in self.__logging_configuration_file_paths():
                if not os.path.exists(maybe_file_path):
                    continue
                else:
                    config_file_path = maybe_file_path

                filesystem.ensure_parent_directories_exist(
                    self.__log_file_path
                )
                logging.config.fileConfig(
                    config_file_path,
                    defaults={'log_file_path': self.__log_file_path},
                    disable_existing_loggers=False
                )

                self.__log_application_start()
                logger.debug(
                    'Loaded logging configuration from %s',
                    config_file_path
                )
                break
        except Exception as e:
            sys.stderr.write(
                'Please run %s with sudo.\n' % self.name
            )
            sys.stderr.flush()
            sys.exit(str(e))

    def __logging_configuration_file_paths(self):
        # Current working directory
        yield constants.LOGGING_CONFIG_FILE_NAME
        # Application specific
        yield (self.__log_file_path + '.ini')
        yield (self.__main_module_path() + '.ini')
        # Global locations
        for dir_path in constants.LOGGING_CONFIG_FILE_DIRECTORIES:
            yield os.path.join(dir_path, constants.LOGGING_CONFIG_FILE_NAME)

    def __main_module_path(self):
        return os.path.abspath(main.__file__)

    def __log_application_start(self):
        LOG_SEPARATOR = '**************************************************'

        logger.debug(LOG_SEPARATOR)
        logger.debug(
            'Starting {name} {version}'.format(
                name=self.name,
                version=self.version
            )
        )
        logger.debug(LOG_SEPARATOR)
        logger.debug('raw arguments = {0}'.format(sys.argv))

    def __exit__(self, exc_type, exception, trace):
        self.exc_type = exc_type
        self.exception = exception
        self.trace = trace

        try:
            if exc_type is None:
                self.__handle_no_exception()
            elif exc_type == SystemExit:
                self.__handle_system_exit()
            else:
                self._handle_error()
                sys.exit(1)
        finally:
            self._exit_cleanup_hook()

    def _exit_cleanup_hook(self):
        logging.shutdown()

    def __handle_no_exception(self):
        logger.debug('Exiting normally')

    def __handle_system_exit(self):
        # Unfortunately a SystemExit can be raised with all kinds of
        # wonky values.  This code attempts to determine the actual
        # exit status.
        code = None
        try:
            # according to the docs a None value for this is equivalent
            # to a 0 value.
            if self.exception is None or self.exception.code is None:
                code = 0
            else:
                code = int(self.exception.code)
        except ValueError:
            code = 1
        except AttributeError:
            # In Python 2.6, the exceptions are passed as strings sometimes.
            # Thus exception.code gets an AttributeError.
            try:
                code = int(self.exception)
            except ValueError:
                code = 1
        except:
            logger.exception("Unknown exception: %s" % str(self.exception))

        if code is not None:
            if code is not 0:
                self._log_exception()
            logger.debug('Application exiting with status %d', code)
        else:
            self._log_exception()
        sys.exit(code)

    def _handle_error(self):
        self._log_exception()
        self.__display_error_message(str(self.exception))

    def __display_error_message(self, message):
        log_file_path = self.__get_root_log_file_path()
        error_message = ''
        if log_file_path:
            error_message += '  More detailed information can be found in '
            error_message += log_file_path
        print >> sys.stderr, message + error_message

    def __get_root_log_file_path(self):
        for handler in logging.root.handlers:
            if isinstance(handler, logging.FileHandler):
                return handler.baseFilename
        return None

    def _log_exception(self):
        formatted_stack_trace = ''.join(
            traceback.format_exception(
                self.exc_type,
                self.exception,
                self.trace
            ) + [ExceptionWithCause.get_cause_if_supported(self.exception)]
        )

        logger.error(
            'Handling uncaught exception: {t}, "{ex}"\n{tb}'.format(
                t=self.exc_type,
                ex=str(self.exception),
                tb=formatted_stack_trace
            )
        )


def entry_point(name, version=None, log_file_path=None,
                application_class=Application):
    """
    A decorator for application entry points.  The decorated function will
    be wrapped in an Application object and executed in that safe environment.
    Note that decorating a function with this decorator will not actually
    cause it to be invoked.  You must explicitly call the function in the
    script.

    Parameters:
        name - human readable name for the application
        version - the version of the application, as a string
        log_file_path - optional name of the log file including whatever
        extension you may want to use.  For example, 'foo.log' would create
        a file called 'foo.log' in the default prestoadmin logging directory
        tree.
        application_class - Type of application to run. The default is
        Application but there can be subclasses of that class.
    """

    def application_decorator(method):
        @functools.wraps(method)
        def wrapped_application(*args, **kwargs):
            with application_class(
                    name,
                    version=version,
                    log_file_path=log_file_path
            ):
                return method(*args, **kwargs)

        return wrapped_application

    return application_decorator
