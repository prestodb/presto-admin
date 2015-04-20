# -*- coding: utf-8 -*-

##
#  This file was copied from Fabric-1.8.0 with some modifications.
#
#  This distribution of fabric is distributed under the following BSD license:
#
#  Copyright (c) 2009, Christian Vest Hansen and Jeffrey E. Forcier
#  All rights reserved.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions are met:
#
#      * Redistributions of source code must retain the above copyright notice,
#        this list of conditions and the following disclaimer.
#      * Redistributions in binary form must reproduce the above copyright
#        notice, this list of conditions and the following disclaimer in the
#        documentation and/or other materials provided with the distribution.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
#  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
#  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
#  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
#  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
#  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
#  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
#  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
#  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
#  POSSIBILITY OF SUCH DAMAGE.
#
##


"""
This module contains Fab's `main` method plus related subroutines.

`main` is executed as the command line ``fab`` program and takes care of
parsing options and commands, loading the user settings file, loading a
fabfile, and executing the commands given.

The other callables defined in this module are internal only. Anything useful
to individuals leveraging Fabric as a library, should be kept elsewhere.
"""
import copy
import getpass
import inspect
import logging
from operator import isMappingType
from optparse import Values, SUPPRESS_HELP
import os
import re
import sys
import types

from configuration import ConfigFileNotFoundError
from prestoadmin import __version__
from prestoadmin.util.application import entry_point
from prestoadmin.util.fabric_application import FabricApplication
from prestoadmin.util.hiddenoptgroup import HiddenOptionGroup
from prestoadmin.util.parser import LoggingOptionParser
import topology

# For checking callables against the API, & easy mocking
from fabric import api, state
from fabric.contrib import console, files, project

from fabric.state import env_options
from fabric.tasks import Task, execute, get_task_details
from fabric.task_utils import _Dict, crawl
from fabric.utils import abort, indent, warn, _pty_size


# One-time calculation of "all internal callables" to avoid doing this on every
# check of a given fabfile callable (in is_classic_task()).
_modules = [api, project, files, console]
_internals = reduce(lambda x, y: x + filter(
    callable,
    vars(y).values()),
    _modules,
    []
)
_LOGGER = logging.getLogger(__name__)


def _get_presto_env_options():
    new_env_options = copy.deepcopy(env_options)
    commands_to_remove = ['fabfile', 'rcfile']
    new_env_options = \
        [x for x in new_env_options if x.dest not in commands_to_remove]
    return new_env_options

presto_env_options = _get_presto_env_options()


# Module recursion cache
class _ModuleCache(object):
    """
    Set-like object operating on modules and storing __name__s internally.
    """
    def __init__(self):
        self.cache = set()

    def __contains__(self, value):
        return value.__name__ in self.cache

    def add(self, value):
        return self.cache.add(value.__name__)

    def clear(self):
        return self.cache.clear()

_seen = _ModuleCache()


def is_classic_task(tup):
    """
    Takes (name, object) tuple, returns True if it's a non-Fab public callable.
    """
    name, func = tup
    try:
        is_classic = (
            callable(func)
            and (func not in _internals)
            and not name.startswith('_')
        )
    # Handle poorly behaved __eq__ implementations
    except (ValueError, TypeError):
        is_classic = False
    return is_classic


def load_fabfile(path, importer=None):
    """
    Import given fabfile path and return (docstring, callables).

    Specifically, the fabfile's ``__doc__`` attribute (a string) and a
    dictionary of ``{'name': callable}`` containing all callables which pass
    the "is a Fabric task" test.
    """
    if importer is None:
        importer = __import__
    # Get directory and fabfile name
    directory, fabfile = os.path.split(path)
    # If the directory isn't in the PYTHONPATH, add it so our import will work
    added_to_path = False
    index = None
    if directory not in sys.path:
        sys.path.insert(0, directory)
        added_to_path = True
    # If the directory IS in the PYTHONPATH, move it to the front temporarily,
    # otherwise other fabfiles -- like Fabric's own -- may scoop the intended
    # one.
    else:
        i = sys.path.index(directory)
        if i != 0:
            # Store index for later restoration
            index = i
            # Add to front, then remove from original position
            sys.path.insert(0, directory)
            del sys.path[i + 1]
    # Perform the import (trimming off the .py)
    imported = importer(os.path.splitext(fabfile)[0])
    # Remove directory from path if we added it ourselves (just to be neat)
    if added_to_path:
        del sys.path[0]
    # Put back in original index if we moved it
    if index is not None:
        sys.path.insert(index + 1, directory)
        del sys.path[0]

    # Actually load tasks
    docstring, new_style, classic, default = load_tasks_from_module(imported)
    tasks = new_style if state.env.new_style_tasks else classic
    # Clean up after ourselves
    _seen.clear()
    return docstring, tasks


def load_tasks_from_module(imported):
    """
    Handles loading all of the tasks for a given `imported` module
    """
    # Obey the use of <module>.__all__ if it is present
    imported_vars = vars(imported)
    if "__all__" in imported_vars:
        imported_vars = [(name, imported_vars[name]) for name in
                         imported_vars if name in imported_vars["__all__"]]
    else:
        imported_vars = imported_vars.items()
    # Return a two-tuple value.  First is the documentation, second is a
    # dictionary of callables only (and don't include Fab operations or
    # underscored callables)
    new_style, classic, default = extract_tasks(imported_vars)
    return imported.__doc__, new_style, classic, default


def extract_tasks(imported_vars):
    """
    Handle extracting tasks from a given list of variables
    """
    new_style_tasks = _Dict()
    classic_tasks = {}
    default_task = None
    if 'new_style_tasks' not in state.env:
        state.env.new_style_tasks = False
    for tup in imported_vars:
        name, obj = tup
        if is_task_object(obj):
            state.env.new_style_tasks = True
            # Use instance.name if defined
            if obj.name and obj.name != 'undefined':
                new_style_tasks[obj.name] = obj
            else:
                obj.name = name
                new_style_tasks[name] = obj
            # Handle aliasing
            if obj.aliases is not None:
                for alias in obj.aliases:
                    new_style_tasks[alias] = obj
            # Handle defaults
            if obj.is_default:
                default_task = obj
        elif is_classic_task(tup):
            classic_tasks[name] = obj
        elif is_task_module(obj):
            docs, newstyle, classic, default = load_tasks_from_module(obj)
            for task_name, task in newstyle.items():
                if name not in new_style_tasks:
                    new_style_tasks[name] = _Dict()
                new_style_tasks[name][task_name] = task
            if default is not None:
                new_style_tasks[name].default = default
    return new_style_tasks, classic_tasks, default_task


def is_task_module(a):
    """
    Determine if the provided value is a task module
    """
    # return (type(a) is types.ModuleType and
    #        any(map(is_task_object, vars(a).values())))
    if isinstance(a, types.ModuleType) and a not in _seen:
        # Flag module as seen
        _seen.add(a)
        # Signal that we need to check it out
        return True


def is_task_object(a):
    """
    Determine if the provided value is a ``Task`` object.

    This returning True signals that all tasks within the fabfile
    module must be Task objects.
    """
    return isinstance(a, Task) and a.use_task_objects


def parser_for_options():
    """
    Handle command-line options with LoggingOptionParser.

    Return parser, largely for use in `parse_arguments`.

    On this parser, you must call parser.parse_args()
    """
    #
    # Initialize
    #

    parser = LoggingOptionParser(
        usage="presto-admin [options] <command> [arg]",
        version="presto-admin %s" % __version__)

    #
    # Define options that don't become `env` vars (typically ones which cause
    # Fabric to do something other than its normal execution, such as
    # --version)
    #

    # Display info about a specific command
    parser.add_option(
        '-d',
        '--display',
        dest='display',
        action='store_true',
        default=False,
        help="print detailed information about command"
    )

    # Control behavior of --list
    list_format_options = ('short', 'normal')
    parser.add_option(
        '-F',
        '--list-format',
        choices=list_format_options,
        default='normal',
        metavar='FORMAT',
        help=SUPPRESS_HELP
    )

    parser.add_option(
        '-I',
        '--initial-password-prompt',
        action='store_true',
        default=False,
        help="Force password prompt up-front"
    )

    # List Fab commands found in loaded fabfiles/source files
    parser.add_option(
        '-l',
        '--list',
        action='store_true',
        dest='list_commands',
        default=False,
        help="print list of possible commands and exit"
    )

    # Like --list, but text processing friendly
    parser.add_option(
        '--shortlist',
        action='store_true',
        dest='shortlist',
        default=False,
        help="print out text processing friendly version of --list"
    )

    # Like --list, but text processing friendly
    parser.add_option(
        '--extended-help',
        action='store_true',
        dest='extended_help',
        default=False,
        help="print out all options, including advanced ones"
    )
    #
    # Add in options which are also destined to show up as `env` vars.
    #

    advanced_options = HiddenOptionGroup(parser, "Advanced Options",
                                         suppress_help=True)

    # Hide most of the options from the help text so it's simpler. Need to
    # document the other options, however.
    commands_to_show = ['hosts', 'exclude_hosts', 'password']
    for option in presto_env_options:
        if option.dest in commands_to_show:
            parser.add_option(option)
        else:
            advanced_options.add_option(option)

    parser.add_option_group(advanced_options)

    # Return parser
    return parser


def _is_task(name, value):
    """
    Is the object a task as opposed to e.g. a dict or int?
    """
    return is_classic_task((name, value)) or is_task_object(value)


def _sift_tasks(mapping):
    tasks, collections = [], []
    for name, value in mapping.iteritems():
        if _is_task(name, value):
            tasks.append(name)
        elif isMappingType(value):
            collections.append(name)
    tasks = sorted(tasks)
    collections = sorted(collections)
    return tasks, collections


def _task_names(mapping):
    """
    Flatten & sort task names in a breadth-first fashion.

    Tasks are always listed before submodules at the same level, but within
    those two groups, sorting is alphabetical.
    """
    tasks, collections = _sift_tasks(mapping)
    for collection in collections:
        module = mapping[collection]
        if hasattr(module, 'default'):
            tasks.append(collection)
        join = lambda x: " ".join((collection, x))
        tasks.extend(map(join, _task_names(module)))
    return tasks


def _print_docstring(docstrings, name):
    if not docstrings:
        return False
    docstring = crawl(name, state.commands).__doc__
    if isinstance(docstring, basestring):
        return docstring


def _normal_list(docstrings=True):
    result = []
    task_names = _task_names(state.commands)
    # Want separator between name, description to be straight col
    max_len = reduce(lambda a, b: max(a, len(b)), task_names, 0)
    sep = '  '
    trail = '...'
    max_width = _pty_size()[1] - 1 - len(trail)
    for name in task_names:
        docstring = _print_docstring(docstrings, name)
        if docstring:
            lines = filter(None, docstring.splitlines())
            first_line = lines[0].strip()
            # Truncate it if it's longer than N chars
            size = max_width - (max_len + len(sep) + len(trail))
            if len(first_line) > size:
                first_line = first_line[:size] + trail
            output = name.ljust(max_len) + sep + first_line
        # Or nothing (so just the name)
        else:
            output = name
        result.append(indent(output))
    return result


COMMANDS_HEADER = "Available commands"


def list_commands(docstring, format_):
    """
    Print all found commands/tasks, then exit. Invoked with ``-l/--list.``

    If ``docstring`` is non-empty, it will be printed before the task list.

    ``format_`` should conform to the options specified in
    ``LIST_FORMAT_OPTIONS``, e.g. ``"short"``, ``"normal"``.
    """
    # Short-circuit with simple short output
    if format_ == "short":
        return _task_names(state.commands)
    # Otherwise, handle more verbose modes
    result = []
    # Docstring at top, if applicable
    if docstring:
        trailer = "\n" if not docstring.endswith("\n") else ""
        result.append(docstring + trailer)
    header = COMMANDS_HEADER
    result.append(header + ":\n")
    c = _normal_list()
    result.extend(c)
    return result


def display_command(name, code=0):
    """
    Print command function's docstring, then exit. Invoked with -d/--display.
    """
    # Sanity check
    command = crawl(name, state.commands)
    name = name.replace(".", " ")
    if command is None:
        msg = "Task '%s' does not appear to exist. Valid task names:\n%s"
        abort(msg % (name, "\n".join(_normal_list(False))))
    # Print out nicely presented docstring if found
    if hasattr(command, '__details__'):
        task_details = command.__details__()
    else:
        task_details = get_task_details(command)

    # Print out "None" if there aren't any arguments; otherwise the text just
    # is "Arguments:\n\n", which is not clear.
    argspec = inspect.getargspec(command.wrapped)
    if len(argspec.args) == 0:
        task_details += 'None'

    if task_details:
        print("Displaying detailed information for task '%s':" % name)
        print('')
        print(indent(task_details, strip=True))
        print('')
    # Or print notice if not
    else:
        print("No detailed information available for task '%s':" % name)
    sys.exit(code)


def parse_arguments(arguments, commands):
    """
    Parse string list into list of tuples: command, args.

    commands is formatted like {'install' : {'server' : WrappedCallable,
    'cli' : WrappedCallable}, 'topology': {'show' : WrappedCallable}}

    Thus, since our arguments are separated by spaces, and is of the form
    ['install', 'server'], we iterate through the commands, progressively
     going deeper into the dict.  If we run out of elements in the dict,
     the rest of the tokens are arguments to the function. If we don't
     get down to the bottom-most level, the command is not valid. If
     at any point the next token is not in the possible_cmd map, the
     command is invalid.
    """

    possible_cmds = commands.copy()
    pos = 0

    while pos < len(arguments):
        if not isinstance(possible_cmds, dict):
            # the rest of are all arguments to the cmd
            break
        if arguments[pos] not in possible_cmds:
            invalid_command_error(arguments)
        possible_cmds = possible_cmds[arguments[pos]]
        pos += 1

    if isinstance(possible_cmds, dict):
        invalid_command_error(arguments)

    cmds = [(".".join(arguments[:pos]), arguments[pos:], {}, [], [], [])]
    return cmds


def invalid_command_error(arguments):
    raise NameError("Command not found:\n%s" % indent(" ".join(arguments)))


def update_output_levels(show, hide):
    """
    Update state.output values as per given comma-separated list of key names.

    For example, ``update_output_levels(show='debug,warnings')`` is
    functionally equivalent to ``state.output['debug'] = True ;
    state.output['warnings'] = True``. Conversely, anything given to ``hide``
    sets the values to ``False``.
    """
    if show:
        for key in show.split(','):
            state.output[key] = True
    if hide:
        for key in hide.split(','):
            state.output[key] = False


def show_commands(docstring, format, code=0):
    print("\n".join(list_commands(docstring, format)))
    sys.exit(code)


def run_tasks(task_list):
    for name, args, kwargs, arg_hosts, arg_roles, arg_excl_hosts in task_list:
        try:
            execute(
                name,
                hosts=state.env.hosts,
                roles=arg_roles,
                exclude_hosts=state.env.exclude_hosts,
                *args, **kwargs
            )
        except TypeError as e:
            if re.match(r".+\(\) takes (at most \d+|no) arguments? "
                        r"\(\d+ given\)", e.message):
                print("Invalid argument(s) to task.\n")
                _LOGGER.exception(e)
                display_command(name, 2)
            else:
                raise
        except BaseException as e:
            raise


def _update_env(options, non_default_options):
    # Fill in the state with the default values
    for option in presto_env_options:
        state.env[option.dest] = getattr(options, option.dest)

    # Load the values from the topology file, if it exists
    load_topology()

    # Go back through and add the non-default values (e.g. the values that
    # were set on the CLI)
    for option in presto_env_options:
        try:
            state.env[option.dest] = getattr(non_default_options, option.dest)
        except AttributeError:
            pass

    # Handle --hosts, --roles, --exclude-hosts (comma separated string =>
    # list)
    for key in ['hosts', 'roles', 'exclude_hosts']:
        if key in state.env and isinstance(state.env[key], basestring):
            state.env[key] = state.env[key].split(',')

    state.output['running'] = False
    update_output_levels(show=options.show, hide=options.hide)


def parse_and_validate_commands(args=sys.argv[1:]):
    # Parse command line options
    parser = parser_for_options()

    # Unless you pass in values, optparse fills in the default values for all
    # of the options. We want to save the version of the options without
    # default values, because that takes precedence over all other env vars.
    non_default_options, arguments = parser.parse_args(args, values=Values())
    options, arguments = parser.parse_args(args)

    # Handle regular args vs -- args
    arguments = parser.largs

    _update_env(options, non_default_options)

    # Find local fabfile path or abort
    fabfile = "prestoadmin"

    # Store absolute path to fabfile in case anyone needs it
    state.env.real_fabfile = fabfile

    # Load fabfile (which calls its module-level code, including
    # tweaks to env values) and put its commands in the shared commands
    # dict
    docstring, callables = load_fabfile(fabfile)
    state.commands.update(callables)

    # Shortlist is now just an alias for the "short" list format;
    # it overrides use of --list-format if somebody were to specify both
    if options.shortlist:
        options.list_format = 'short'
        options.list_commands = True

    if len(parser.rargs) > 0:
        warn("Arbitrary remote shell commands not supported.")
        show_commands(None, options.list_format, 2)

    # List available commands
    if options.list_commands:
        show_commands(docstring, options.list_format)

    if options.extended_help:
        parser.print_extended_help()
        sys.exit(0)

    # If user didn't specify any commands to run, show help
    if not arguments:
        parser.print_help()
        sys.exit(0)  # don't consider this an error

    # Parse arguments into commands to run (plus args/kwargs/hosts)
    commands_to_run = None
    try:
        commands_to_run = parse_arguments(arguments, state.commands)
    except NameError as e:
        warn(e.message)
        _LOGGER.exception(e)
        show_commands(None, options.list_format, 2)

    # Handle show (command-specific help) option
    if options.display:
        display_command(commands_to_run[0][0])

    # Initial password prompt, if requested
    if options.initial_password_prompt:
        prompt = "Initial value for env.password: "
        state.env.password = getpass.getpass(prompt)

    state.env['tasks'] = [x[0] for x in commands_to_run]

    return commands_to_run


def load_topology():
    try:
        topology.set_env_from_conf()
    except ConfigFileNotFoundError as e:
        # If there is no topology file, just store empty
        # roledefs for now and save the error in the environment variables.
        # If the task is an install task, we will set up a prompt for the
        # user to interactively enter the config vars. Else, we will error
        # out at a later point.
        state.env['topology_config_not_found'] = e
        pass


@entry_point('Presto Admin', version=__version__,
             log_file_path="presto-admin.log",
             application_class=FabricApplication)
def main(args=sys.argv[1:]):
    """
    Main command-line execution loop.
    """
    commands_to_run = parse_and_validate_commands(args)

    names = ", ".join(x[0] for x in commands_to_run)
    _LOGGER.debug("Commands to run: %s" % names)

    # At this point all commands must exist, so execute them in order.
    run_tasks(commands_to_run)


if __name__ == "__main__":
    main()
