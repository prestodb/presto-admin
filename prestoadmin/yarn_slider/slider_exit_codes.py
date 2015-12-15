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
Slider client exit codes. Taken from the slider online documentation.
"""

#
# 0: success
#
EXIT_SUCCESS = 0

#
# -1: generic "false" response. The operation worked but
# the result was not true
#
EXIT_FALSE = -1

#
# Exit code when a client requested service termination: {@value}
#
EXIT_CLIENT_INITIATED_SHUTDOWN = 1

#
# Exit code when targets could not be launched: {@value}
#
EXIT_TASK_LAUNCH_FAILURE = 2

#
# Exit code when a control-C, kill -3, signal was picked up: {@value}
#
EXIT_INTERRUPTED = 3

#
# Exit code when a usage message was printed: {@value}
#
EXIT_USAGE = 4

#
# Exit code when something happened but we can't be specific: {@value}
#
EXIT_OTHER_FAILURE = 5

#
# Exit code on connectivity problems: {@value}
#
EXIT_MOVED = 31

#
# found: {@value}.
# <p>
# This is low value as in HTTP it is normally a success/redirect
# whereas on the command line 0 is the sole success code.
# <p>
# <code>302 Found</code>
#
EXIT_FOUND = 32

#
# Exit code on a request where the destination has not changed
# and (somehow) the command specified that this is an error.
# That is, this exit code is somehow different from a "success"
# : {@value}
# <p>
# <code>304 Not Modified </code>
#
EXIT_NOT_MODIFIED = 34

#
# Exit code when the command line doesn't parse: {@value}, or
# when it is otherwise invalid.
# <p>
# <code>400 BAD REQUEST</code>
#
EXIT_COMMAND_ARGUMENT_ERROR = 40

#
# The request requires user authentication: {@value}
# <p>
# <code>401 Unauthorized</code>
#
EXIT_UNAUTHORIZE = 41

#
# Forbidden action: {@value}
# <p>
# <code>403: Forbidden</code>
#
EXIT_FORBIDDEN = 43

#
# Something was not found: {@value}
# <p>
# <code>404: NOT FOUND</code>
#
EXIT_NOT_FOUND = 44

#
# The operation is not allowed: {@value}
# <p>
# <code>405: NOT ALLOWED</code>
#
EXIT_OPERATION_NOT_ALLOWED = 45

#
# The command is somehow not acceptable: {@value}
# <p>
# <code>406: NOT ACCEPTABLE</code>
#
EXIT_NOT_ACCEPTABLE = 46

#
# Exit code on connectivity problems: {@value}
# <p>
# <code>408: Request Timeout</code>
#
EXIT_CONNECTIVITY_PROBLEM = 48

#
# The request could not be completed due to a conflict with the current
# state of the resource.  {@value}
# <p>
# <code>409: conflict</code>
#
EXIT_CONFLICT = 49

#
# internal error: {@value}
# <p>
# <code>500 Internal Server Error</code>
#
EXIT_INTERNAL_ERROR = 50

#
# Unimplemented feature: {@value}
# <p>
# <code>501: Not Implemented</code>
#
EXIT_UNIMPLEMENTED = 51

#
# Service Unavailable it may be available later: {@value}
# <p>
# <code>503 Service Unavailable</code>
#
EXIT_SERVICE_UNAVAILABLE = 53

#
# The application does not support, or refuses to support this version:
# {@value}.
#
# If raised, this is expected to be raised server-side and likely due
# to client/server version incompatibilities.
# <p>
# <code> 505: Version Not Supported</code>
#
EXIT_UNSUPPORTED_VERSION = 55

#
# Exit code when an exception was thrown from the service: {@value}
# <p>
# <code>5XX</code>
#
EXIT_EXCEPTION_THROWN = 56

#
# service entered the failed state: {@value}
#
EXIT_YARN_SERVICE_FAILED = 65

#
# service was killed: {@value}
#
EXIT_YARN_SERVICE_KILLED = 66

#
# timeout on monitoring client: {@value}
#
EXIT_TIMED_OUT = 67

#
# service finished with an error: {@value}
#
EXIT_YARN_SERVICE_FINISHED_WITH_ERROR = 68

#
# the application instance is unknown: {@value}
#
EXIT_UNKNOWN_INSTANCE = 69

#
# the application instance is in the wrong state for that operation: {@value}
#
EXIT_BAD_STATE = 70

#
# A spawned master process failed
#
EXIT_PROCESS_FAILED = 71

#
# The instance failed -too many containers were
# failing or some other threshold was reached
#
EXIT_DEPLOYMENT_FAILED = 72

#
# The application is live -and the requested operation
# does not work if the cluster is running
#
EXIT_APPLICATION_IN_USE = 73

#
# There already is an application instance of that name
# when an attempt is made to create a new instance
#
EXIT_INSTANCE_EXISTS = 75

#
# Exit code when the configurations in valid/incomplete: {@value}
#
EXIT_BAD_CONFIGURATION = 77
