# -*- coding: utf-8 -*-

##
# This file was copied from Fabric-1.8.0 with some modifications.
#
# This distribution of fabric is distributed under the following BSD license:
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
import logging
from operator import isMappingType
from optparse import Values, SUPPRESS_HELP
import os
import sys
import textwrap
import types

# For checking callables against the API, & easy mocking
from fabric import api, state
from fabric.contrib import console, files, project

from fabric.state import env_options
from fabric.tasks import Task, execute
from fabric.task_utils import _Dict, crawl
from fabric.utils import abort, indent, warn, _pty_size

from prestoadmin.util.exception import ConfigurationError, is_arguments_error
from prestoadmin import __version__
from prestoadmin.util.application import entry_point
from prestoadmin.util.fabric_application import FabricApplication
from prestoadmin.util.hiddenoptgroup import HiddenOptionGroup
from prestoadmin.util.parser import LoggingOptionParser

# One-time calculation of "all internal callables" to avoid doing this on every
# check of a given fabfile callable (in is_classic_task()).
_modules = [api, project, files, console]
_internals = reduce(lambda x, y: x + filter(callable, vars(y).values()),
                    _modules, [])
_LOGGER = logging.getLogger(__name__)


def _get_presto_env_options():
    new_env_options = copy.deepcopy(env_options)
    commands_to_remove = ['fabfile', 'parallel', 'rcfile', 'skip_bad_hosts',
                          'warn_only', 'always_use_pty', 'skip_unknown_tasks',
                          'abort_on_prompts', 'pool_size',
                          'eagerly_disconnect', 'ssh_config_path']
    commands_to_hide = ['--roles', '--shell', '--linewise', '--show', '--hide']
    new_env_options = \
        [x for x in new_env_options if x.dest not in commands_to_remove]
    for env_option in new_env_options:
        if env_option.get_opt_string() in commands_to_hide:
            env_option.help = SUPPRESS_HELP
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
            callable(func) and (func not in _internals) and not
            name.startswith('_')
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
        usage='presto-admin [options] <command> [arg]',
        version='presto-admin %s' % __version__,
        epilog='\n' + '\n'.join(list_commands(None, 'normal')))

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
        help='print detailed information about command'
    )

    parser.add_option(
        '--extended-help',
        action='store_true',
        dest='extended_help',
        default=False,
        help='print out all options, including advanced ones'
    )

    parser.add_option(
        '-I',
        '--initial-password-prompt',
        action='store_true',
        default=False,
        help="Force password prompt up-front"
    )

    parser.add_option(
        '--nodeps',
        action='store_true',
        dest='nodeps',
        default=False,
        help=SUPPRESS_HELP
    )

    parser.add_option(
        '--force',
        action='store_true',
        dest='force',
        default=False,
        help=SUPPRESS_HELP
    )

    #
    # Add in options which are also destined to show up as `env` vars.
    #

    advanced_options = HiddenOptionGroup(parser, "Advanced Options",
                                         suppress_help=True)

    # Hide most of the options from the help text so it's simpler. Need to
    # document the other options, however.
    commands_to_show = ['password']

    for option in presto_env_options:
        if option.dest in commands_to_show:
            parser.add_option(option)
        else:
            advanced_options.add_option(option)

    advanced_options.add_option(
        '--serial',
        action='store_true',
        dest='serial',
        default=False,
        help="default to serial execution method"
    )

    # Allow setting of arbitrary env vars at runtime.
    advanced_options.add_option(
        '--set',
        metavar="KEY=VALUE,...",
        dest='env_settings',
        default="",
        help=SUPPRESS_HELP
    )

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
        tasks.extend(map(lambda x: " ".join((collection, x)),
                     _task_names(module)))
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


COMMANDS_HEADER = 'Commands:'


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
    result.append(header)
    c = _normal_list()
    result.extend(c)
    result.extend("\n")
    return result


def get_task_docstring(task):
    details = [
        textwrap.dedent(task.__doc__)
        if task.__doc__
        else 'No docstring provided']

    return '\n'.join(details)


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
    # get the presented docstring if found
    task_details = get_task_docstring(command)

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
            nodeps_tasks = ['package.install', 'server.uninstall',
                            'server.install', 'server.upgrade']
            if state.env.nodeps and name.strip() not in nodeps_tasks:
                sys.stderr.write('Invalid argument --nodeps to task: %s\n'
                                 % name)
                display_command(name, 2)

            return execute(
                name,
                hosts=state.env.hosts,
                roles=arg_roles,
                exclude_hosts=state.env.exclude_hosts,
                *args, **kwargs
            )
        except TypeError as e:
            if is_arguments_error(e):
                print("Incorrect number of arguments to task.\n")
                _LOGGER.error('Incorrect number of arguments to task',
                              exc_info=True)
                display_command(name, 2)
            else:
                raise
        except BaseException as e:
            raise


def _escape_split(sep, argstr):
    """
    Allows for escaping of the separator: e.g. task:arg='foo\, bar'

    It should be noted that the way bash et. al. do command line parsing, those
    single quotes are required.
    """
    escaped_sep = r'\%s' % sep

    if escaped_sep not in argstr:
        return argstr.split(sep)

    before, _, after = argstr.partition(escaped_sep)
    startlist = before.split(sep)  # a regular split is fine here
    unfinished = startlist[-1]
    startlist = startlist[:-1]

    # recurse because there may be more escaped separators
    endlist = _escape_split(sep, after)

    # finish building the escaped value. we use endlist[0] becaue the first
    # part of the string sent in recursion is the rest of the escaped value.
    unfinished += sep + endlist[0]

    return startlist + [unfinished] + endlist[1:]  # put together all the parts


def _to_boolean(string):
    """
    Parses the given string into a boolean.  If its already a boolean, its
    returned unchanged.

    This method does strict parsing; only the string "True" returns the boolean
    True, and only the string "False" returns the boolean False.  All other
    values throw a ValueError.

    Args:
        string: the string to parse
    """
    if string is True or string == 'True':
        return True
    elif string is False or string == 'False':
        return False

    raise ValueError("invalid boolean string: %s" % string)


def _handle_generic_set_env_vars(non_default_options):
    if not hasattr(non_default_options, 'env_settings'):
        return non_default_options

    # Allow setting of arbitrary env keys.
    # This comes *before* the "specific" env_options so that those may
    # override these ones. Specific should override generic, if somebody
    # was silly enough to specify the same key in both places.
    # E.g. "fab --set shell=foo --shell=bar" should have env.shell set to
    # 'bar', not 'foo'.
    for pair in _escape_split(',', non_default_options.env_settings):
        pair = _escape_split('=', pair)
        # "--set x" => set env.x to True
        # "--set x=" => set env.x to ""
        key = pair[0]
        value = True
        if len(pair) == 2:
            try:
                value = _to_boolean(pair[1])
            except ValueError:
                value = pair[1]
        state.env[key] = value

    non_default_options_dict = vars(non_default_options)
    del non_default_options_dict['env_settings']
    return Values(non_default_options_dict)


def validate_hosts(cli_hosts, config_path):
    # If there's no config file to validate against, don't. This would happen
    # in the case of a task that doesn't define a callback that loads config.
    if config_path is None:
        return

    # At this point, state.env.conf_hosts contains the hosts that we loaded
    # from the configuration, if any.
    cli_host_set = set(cli_hosts.split(','))
    if 'conf_hosts' in state.env:
        conf_hosts = set(state.env.conf_hosts)
        if not cli_host_set.issubset(conf_hosts):
            raise ConfigurationError('Hosts defined in --hosts/-H must be '
                                     'present in %s' % (config_path))
    else:
        raise ConfigurationError(
            'Hosts cannot be defined with --hosts/-H when no hosts are listed '
            'in the configuration file %s. Correct the configuration file or '
            'run the command again without the --hosts or -H option.' %
            config_path)


def _update_env(default_options, non_default_options, load_config_callback):
    # Fill in the state with the default values
    for opt, value in default_options.__dict__.items():
        state.env[opt] = value

    if load_config_callback:
        config_path = load_config(load_config_callback)
    else:
        config_path = None

    # Save env.hosts from the config into another env variable for validation.
    # _handle_generic_set_env_vars will overwrite it if --set hosts=...
    # is present.
    if state.env.hosts:
        state.env.conf_hosts = state.env.hosts

    non_default_options = _handle_generic_set_env_vars(non_default_options)

    if isinstance(state.env.hosts, basestring):
        # Take advantage of the fact that if there was a generic --set option
        # for hosts, it's still an unsplit, comma separated string rather than
        # a list, which is what it would be after loading hosts from a config
        # file.
        validate_hosts(state.env.hosts, config_path)

    # Go back through and add the non-default values (e.g. the values that
    # were set on the CLI)
    for opt, value in non_default_options.__dict__.items():
        # raise error if hosts not in topology
        if opt == 'hosts':
            validate_hosts(value, config_path)

        state.env[opt] = value

    # Handle --hosts, --roles, --exclude-hosts (comma separated string =>
    # list)
    for key in ['hosts', 'roles', 'exclude_hosts']:
        if key in state.env and isinstance(state.env[key], basestring):
            state.env[key] = state.env[key].split(',')

    state.output['running'] = False
    state.output['status'] = False
    update_output_levels(show=state.env.show, hide=state.env.hide)
    state.env.skip_bad_hosts = True

    # env.conf_hosts is an implementation detail of the option parsing and
    # validation. Hide it from the world.
    if 'conf_hosts' in state.env:
        del state.env['conf_hosts']


def get_default_options(options, non_default_options):
    """
    Given a dictionary of options containing the defaults optparse has filled
    in, and a dictionary of options containing only options parsed from the
    command line, returns a dictionary containing the default options that
    remain after removing the default options that were overridden by the
    options passed on the command line.

    Mathematically, this returns a dictionary with
    default_options.keys = options.keys() \ non_default_options.keys()
    where \ is the set difference operator.
    The value of a key present in default_options is the value of the same key
    in options.
    """
    options_dict = vars(options)
    non_default_options_dict = vars(non_default_options)
    default_options = Values(dict((k, options_dict[k]) for k in options_dict
                                  if k not in non_default_options_dict))
    return default_options


def _get_config_callback(commands_to_run):
    config_callback = None
    if len(commands_to_run) != 1:
        raise Exception('Multiple commands are not supported')

    c = commands_to_run[0][0]
    module, command = c.split('.')

    module_dict = state.commands[module]
    command_callable = module_dict[command]

    try:
        config_callback = command_callable.pa_config_callback
    except AttributeError:
        pass

    return config_callback


def parse_and_validate_commands(args=sys.argv[1:]):
    # Find local fabfile path or abort
    fabfile = "prestoadmin"

    # Store absolute path to fabfile in case anyone needs it
    state.env.real_fabfile = fabfile

    # Load fabfile (which calls its module-level code, including
    # tweaks to env values) and put its commands in the shared commands
    # dict
    docstring, callables = load_fabfile(fabfile)
    state.commands.update(callables)

    # Parse command line options
    parser = parser_for_options()

    # Unless you pass in values, optparse fills in the default values for all
    # of the options. We want to save the version of the options without
    # default values, because that takes precedence over all other env vars.
    non_default_options, arguments = parser.parse_args(args, values=Values())
    options, arguments = parser.parse_args(args)
    default_options = get_default_options(options, non_default_options)

    # Handle regular args vs -- args
    arguments = parser.largs

    if len(parser.rargs) > 0:
        warn("Arbitrary remote shell commands not supported.")
        show_commands(None, 'normal', 2)

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
        _LOGGER.warn("Unable to parse arguments", exc_info=True)
        parser.print_help()
        sys.exit(2)

    # Handle show (command-specific help) option
    if options.display:
        display_command(commands_to_run[0][0])

    load_config_callback = _get_config_callback(commands_to_run)
    _update_env(default_options, non_default_options, load_config_callback)

    if not options.serial:
        state.env.parallel = True

    state.env.warn_only = False

    # Initial password prompt, if requested
    if options.initial_password_prompt:
        prompt = "Initial value for env.password: "
        state.env.password = getpass.getpass(prompt)

    state.env['tasks'] = [x[0] for x in commands_to_run]

    return commands_to_run


def load_config(load_config_callback):
    """
    This provides a patch point for the unit tests so that individual test
    cases don't need to know the internal details of what happens in
    _load_topology. See test_main.py for examples.
    """
    return load_config_callback()


def _exit_code(results):
    """
    results from run_tasks take the form of a dict with one or more entries
    hostname: Exception | None

    If every entry in the dict has a value of None, the exit code is 0.
    If any entry has a value that is not None, something failed, and we should
    exit with a non-zero exit code.

    That isn't really the whole story: Any task that calls tasks.execute() and
    returns that as the result will have an item in the dictionary of the form
    hostname: {hostname: Exception | None}. This means that we need to
    recursively check any values in the map that are of type dict following the
    above scheme.
    """
    for v in results.values():
        # No exception, inspect the next value.
        if v is None:
            continue

        # The value is a dict resulting from calling fabric.tasks.execute.
        # Check the results recursively
        if type(v) is dict:
            exit_code = _exit_code(v)
            if exit_code != 0:
                return exit_code
            continue

        # In any case where things were OK above, we've continued the loop. At
        # this point, we know something failed.
        return 1
    return 0


@entry_point('presto-admin', version=__version__,
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
    return _exit_code(run_tasks(commands_to_run))


if __name__ == "__main__":
    sys.exit(main())
