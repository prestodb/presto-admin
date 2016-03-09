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

"""Monkey patches needed to change logging and error handling in Fabric"""
import traceback
import sys
import logging
from traceback import format_exc

from fabric import state
from fabric.context_managers import settings
from fabric.exceptions import NetworkError
from fabric.job_queue import JobQueue
from fabric.tasks import _is_task, WrappedCallableTask, requires_parallel
from fabric.task_utils import crawl, parse_kwargs
from fabric.utils import error
import fabric.api
import fabric.operations
import fabric.tasks
from fabric.network import needs_host, to_dict, disconnect_all

from prestoadmin.util import exception


_LOGGER = logging.getLogger(__name__)
old_warn = fabric.utils.warn
old_abort = fabric.utils.abort
old_run = fabric.operations.run
old_sudo = fabric.operations.sudo


# Need to monkey patch Fabric's warn method in order to print out
# all exceptions seen to the logs.
def warn(msg):
    if fabric.api.env.host:
        msg = '[' + fabric.api.env.host + '] ' + msg
    old_warn(msg)
    _LOGGER.warn(msg + '\n\n' + format_exc())

fabric.utils.warn = warn
fabric.api.warn = warn


def abort(msg):
    if fabric.api.env.host:
        msg = '[' + fabric.api.env.host + '] ' + msg
    old_abort(msg)

fabric.utils.abort = abort
fabric.api.abort = abort


# Monkey patch run and sudo so that the stdout and stderr
# also go to the logs.
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


fabric.operations.run = run
fabric.api.run = run


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


fabric.operations.sudo = sudo
fabric.api.sudo = sudo


def log_output(out):
    _LOGGER.info('\nCOMMAND: ' + out.command + '\nFULL COMMAND: ' +
                 out.real_command + '\nSTDOUT: ' + out + '\nSTDERR: ' +
                 out.stderr)


# Monkey patch _execute and execute so that we can handle errors differently
def _execute(task, host, my_env, args, kwargs, jobs, queue, multiprocessing):
    """
    Primary single-host work body of execute().
    """
    # Log to stdout
    if state.output.running and not hasattr(task, 'return_value'):
        print("[%s] Executing task '%s'" % (host, my_env['command']))
    # Create per-run env with connection settings
    local_env = to_dict(host)
    local_env.update(my_env)
    # Set a few more env flags for parallelism
    if queue is not None:
        local_env.update({'parallel': True, 'linewise': True})
    # Handle parallel execution
    if queue is not None:  # Since queue is only set for parallel
        name = local_env['host_string']

        # Wrap in another callable that:
        # * expands the env it's given to ensure parallel, linewise, etc are
        # all set correctly and explicitly. Such changes are naturally
        # insulted from the parent process.
        # * nukes the connection cache to prevent shared-access problems
        # * knows how to send the tasks' return value back over a Queue
        # * captures exceptions raised by the task
        def inner(args, kwargs, queue, name, env):
            state.env.update(env)

            def submit(result):
                queue.put({'name': name, 'result': result})

            try:
                state.connections.clear()
                submit(task.run(*args, **kwargs))
            except BaseException, e:
                _LOGGER.error(traceback.format_exc())
                submit(e)
                sys.exit(1)

        # Stuff into Process wrapper
        kwarg_dict = {
            'args': args,
            'kwargs': kwargs,
            'queue': queue,
            'name': name,
            'env': local_env,
        }
        p = multiprocessing.Process(target=inner, kwargs=kwarg_dict)
        # Name/id is host string
        p.name = name
        # Add to queue
        jobs.append(p)
    # Handle serial execution
    else:
        with settings(**local_env):
            return task.run(*args, **kwargs)


def execute(task, *args, **kwargs):
    """
    Patched version of fabric's execute task with alternative error handling
    """
    my_env = {'clean_revert': True}
    results = {}
    # Obtain task
    is_callable = callable(task)
    if not (is_callable or _is_task(task)):
        # Assume string, set env.command to it
        my_env['command'] = task
        task = crawl(task, state.commands)
        if task is None:
            msg = "%r is not callable or a valid task name" % (
                my_env['command'],)
            if state.env.get('skip_unknown_tasks', False):
                warn(msg)
                return
            else:
                abort(msg)
    # Set env.command if we were given a real function or callable task obj
    else:
        dunder_name = getattr(task, '__name__', None)
        my_env['command'] = getattr(task, 'name', dunder_name)
    # Normalize to Task instance if we ended up with a regular callable
    if not _is_task(task):
        task = WrappedCallableTask(task)
    # Filter out hosts/roles kwargs
    new_kwargs, hosts, roles, exclude_hosts = parse_kwargs(kwargs)
    # Set up host list
    my_env['all_hosts'], my_env[
        'effective_roles'] = task.get_hosts_and_effective_roles(hosts, roles,
                                                                exclude_hosts,
                                                                state.env)

    parallel = requires_parallel(task)
    if parallel:
        # Import multiprocessing if needed, erroring out usefully
        # if it can't.
        try:
            import multiprocessing
        except ImportError:
            import traceback

            tb = traceback.format_exc()
            abort(tb + """
    At least one task needs to be run in parallel, but the
    multiprocessing module cannot be imported (see above
    traceback.) Please make sure the module is installed
    or that the above ImportError is fixed.""")
    else:
        multiprocessing = None

    # Get pool size for this task
    pool_size = task.get_pool_size(my_env['all_hosts'], state.env.pool_size)
    # Set up job queue in case parallel is needed
    queue = multiprocessing.Queue() if parallel else None
    jobs = JobQueue(pool_size, queue)
    if state.output.debug:
        jobs._debug = True

    # Call on host list
    if my_env['all_hosts']:
        # Attempt to cycle on hosts, skipping if needed
        for host in my_env['all_hosts']:
            try:
                results[host] = _execute(
                    task, host, my_env, args, new_kwargs, jobs, queue,
                    multiprocessing
                )
            except NetworkError, e:
                results[host] = e
                # Backwards compat test re: whether to use an exception or
                # abort
                if state.env.skip_bad_hosts or state.env.warn_only:
                    func = warn
                else:
                    func = abort
                error(e.message, func=func, exception=e.wrapped)
            except SystemExit, e:
                results[host] = e

            # If requested, clear out connections here and not just at the end.
            if state.env.eagerly_disconnect:
                disconnect_all()

        # If running in parallel, block until job queue is emptied
        if jobs:
            jobs.close()
            # Abort if any children did not exit cleanly (fail-fast).
            # This prevents Fabric from continuing on to any other tasks.
            # Otherwise, pull in results from the child run.
            ran_jobs = jobs.run()
            for name, d in ran_jobs.iteritems():
                if d['exit_code'] != 0:
                    if isinstance(d['results'], NetworkError):
                        func = warn if state.env.skip_bad_hosts \
                            or state.env.warn_only else abort
                        error(d['results'].message,
                              exception=d['results'].wrapped, func=func)
                    elif exception.is_arguments_error(d['results']):
                        raise d['results']
                    elif isinstance(d['results'], SystemExit):
                        # System exit indicates abort
                        pass
                    elif isinstance(d['results'], BaseException):
                        error(d['results'].message, exception=d['results'])
                    else:
                        error('One or more hosts failed while executing task.')
                results[name] = d['results']

    # Or just run once for local-only
    else:
        with settings(**my_env):
            results['<local-only>'] = task.run(*args, **new_kwargs)
    # Return what we can from the inner task executions

    return results


fabric.tasks._execute = _execute
fabric.tasks.execute = execute
