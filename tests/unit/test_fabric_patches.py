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
import sys
import logging

from fabric import state
from fabric.context_managers import hide, settings
from fabric.decorators import hosts, parallel, roles, serial
from fabric.exceptions import NetworkError
from fabric.tasks import Task
from fudge import Fake, patched_context, with_fakes, clear_expectations
from fabric.state import env
import fabric.api
import fabric.operations
import fabric.utils
from mock import call
from mock import patch
from tests.base_test_case import BaseTestCase

from prestoadmin.util.application import Application
from prestoadmin.fabric_patches import execute


APPLICATION_NAME = 'foo'


@patch('prestoadmin.util.application.filesystem')
@patch('prestoadmin.util.application.logging.config')
class FabricPatchesTest(BaseTestCase):

    def setUp(self):
        # basicConfig is a noop if there are already handlers
        # present on the root logger, remove them all here
        self.__old_log_handlers = []
        for handler in logging.root.handlers:
            self.__old_log_handlers.append(handler)
            logging.root.removeHandler(handler)
        # Load prestoadmin so that the monkeypatching is in place
        BaseTestCase.setUp(self, capture_output=True)

    def tearDown(self):
        # restore the old log handlers
        for handler in logging.root.handlers:
            logging.root.removeHandler(handler)
        for handler in self.__old_log_handlers:
            logging.root.addHandler(handler)
        BaseTestCase.tearDown(self)

    @patch('prestoadmin.fabric_patches._LOGGER')
    def test_warn_api_prints_out_message(self, logger_mock, log_conf_mock,
                                         filesystem_mock):
        with Application(APPLICATION_NAME):
            fabric.api.warn("Test warning.")

        logger_mock.warn.assert_has_calls(
            [
                call('Test warning.\n\nNone\n'),
            ]
        )
        self.assertEqual(
            '\nWarning: Test warning.\n\n',
            self.test_stderr.getvalue()
        )

    @patch('prestoadmin.fabric_patches._LOGGER')
    def test_warn_utils_prints_out_message(self, logger_mock, log_conf_mock,
                                           filesystem_mock):
        with Application(APPLICATION_NAME):
            fabric.utils.warn("Test warning.")

        logger_mock.warn.assert_has_calls(
            [
                call('Test warning.\n\nNone\n'),
                ]
        )
        self.assertEqual(
            '\nWarning: Test warning.\n\n',
            self.test_stderr.getvalue()
        )

    @patch('prestoadmin.fabric_patches._LOGGER')
    def test_warn_utils_prints_out_message_with_host(self, logger_mock,
                                                     log_conf_mock, fs_mock):
        fabric.api.env.host = 'host'
        with Application(APPLICATION_NAME):
            fabric.utils.warn("Test warning.")

        logger_mock.warn.assert_has_calls(
            [
                call('[host] Test warning.\n\nNone\n'),
                ]
        )
        self.assertEqual(
            '\nWarning: [host] Test warning.\n\n',
            self.test_stderr.getvalue()
        )

    @patch('fabric.operations._run_command')
    @patch('prestoadmin.fabric_patches._LOGGER')
    def test_run_api_logs_stdout(self, logger_mock, run_command_mock,
                                 logging_config_mock, filesystem_mock):
        self._execute_operation_test(run_command_mock, logger_mock,
                                     fabric.api.run)

    @patch('fabric.operations._run_command')
    @patch('prestoadmin.fabric_patches._LOGGER')
    def test_run_op_logs_stdout(self, logger_mock, run_command_mock,
                                logging_config_mock, filesystem_mock):
        self._execute_operation_test(run_command_mock, logger_mock,
                                     fabric.operations.run)

    @patch('fabric.operations._run_command')
    @patch('prestoadmin.fabric_patches._LOGGER')
    def test_sudo_api_logs_stdout(self, logger_mock, run_command_mock,
                                  logging_config_mock, filesystem_mock):
        self._execute_operation_test(run_command_mock, logger_mock,
                                     fabric.api.sudo)

    @patch('fabric.operations._run_command')
    @patch('prestoadmin.fabric_patches._LOGGER')
    def test_sudo_op_logs_stdout(self, logger_mock, run_command_mock,
                                 logging_config_mock, filesystem_mock):
        self._execute_operation_test(run_command_mock, logger_mock,
                                     fabric.operations.sudo)

    def _execute_operation_test(self, run_command_mock, logger_mock, func):
        out = fabric.operations._AttributeString('Test warning')
        out.command = 'echo "Test warning"'
        out.real_command = '/bin/bash echo "Test warning"'
        out.stderr = ''
        run_command_mock.return_value = out

        fabric.api.env.host_string = 'localhost'
        with Application(APPLICATION_NAME):
            func('echo "Test warning"')
            pass

        logger_mock.info.assert_has_calls(
            [
                call('\nCOMMAND: echo "Test warning"\nFULL COMMAND: /bin/bash'
                     ' echo "Test warning"\nSTDOUT: Test warning\nSTDERR: '),
                ]
        )


# Most of these tests were taken or modified from fabric's test_tasks.py
# Below is the license for the fabric code:
# Copyright (c) 2009-2015 Jeffrey E. Forcier
# Copyright (c) 2008-2009 Christian Vest Hansen
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice,
#       this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
class TestExecute(BaseTestCase):
    def setUp(self):
        clear_expectations()
        super(TestExecute, self).setUp(capture_output=True)

    @with_fakes
    def test_calls_task_function_objects(self):
        """
        should execute the passed-in function object
        """
        execute(Fake(callable=True, expect_call=True))

    @with_fakes
    def test_should_look_up_task_name(self):
        """
        should also be able to handle task name strings
        """
        name = 'task1'
        commands = {name: Fake(callable=True, expect_call=True)}
        with patched_context(fabric.state, 'commands', commands):
            execute(name)

    @with_fakes
    def test_should_handle_name_of_Task_object(self):
        """
        handle corner case of Task object referrred to by name
        """
        name = 'task2'

        class MyTask(Task):
            run = Fake(callable=True, expect_call=True)
        mytask = MyTask()
        mytask.name = name
        commands = {name: mytask}
        with patched_context(fabric.state, 'commands', commands):
            execute(name)

    def test_should_abort_if_task_name_not_found(self):
        """
        should abort if given an invalid task name
        """
        self.assertRaisesRegexp(SystemExit,
                                "'thisisnotavalidtaskname' is not callable or"
                                " a valid task name",
                                execute, 'thisisnotavalidtaskname')

    def test_should_not_abort_if_task_name_not_found_with_skip(self):
        """
        should not abort if given an invalid task name
        and skip_unknown_tasks in env
        """
        env.skip_unknown_tasks = True
        execute('thisisnotavalidtaskname')
        del env['skip_unknown_tasks']

    @with_fakes
    def test_should_pass_through_args_kwargs(self):
        """
        should pass in any additional args, kwargs to the given task.
        """
        task = (
            Fake(callable=True, expect_call=True)
            .with_args('foo', biz='baz')
        )
        execute(task, 'foo', biz='baz')

    @with_fakes
    def test_should_honor_hosts_kwarg(self):
        """
        should use hosts kwarg to set run list
        """
        # Make two full copies of a host list
        hostlist = ['a', 'b', 'c']
        hosts = hostlist[:]

        # Side-effect which asserts the value of env.host_string when it runs
        def host_string():
            self.assertEqual(env.host_string, hostlist.pop(0))
        task = Fake(callable=True, expect_call=True).calls(host_string)
        with hide('everything'):
            execute(task, hosts=hosts)

    def test_should_honor_hosts_decorator(self):
        """
        should honor @hosts on passed-in task objects
        """
        # Make two full copies of a host list
        hostlist = ['a', 'b', 'c']

        @hosts(*hostlist[:])
        def task():
            self.assertEqual(env.host_string, hostlist.pop(0))
        with hide('running'):
            execute(task)

    def test_should_honor_roles_decorator(self):
        """
        should honor @roles on passed-in task objects
        """
        # Make two full copies of a host list
        roledefs = {'role1': ['a', 'b', 'c'], 'role2': ['d', 'e']}
        role_copy = roledefs['role1'][:]

        @roles('role1')
        def task():
            self.assertEqual(env.host_string, role_copy.pop(0))
        with settings(hide('running'), roledefs=roledefs):
            execute(task)

    @with_fakes
    def test_should_set_env_command_to_string_arg(self):
        """
        should set env.command to any string arg, if given
        """
        name = "foo"

        def command():
            self.assert_(env.command, name)
        task = Fake(callable=True, expect_call=True).calls(command)
        with patched_context(fabric.state, 'commands', {name: task}):
            execute(name)

    @with_fakes
    def test_should_set_env_command_to_name_attr(self):
        """
        should set env.command to TaskSubclass.name if possible
        """
        name = "foo"

        def command():
            self.assertEqual(env.command, name)
        task = (
            Fake(callable=True, expect_call=True)
            .has_attr(name=name)
            .calls(command)
        )
        execute(task)

    @with_fakes
    def test_should_set_all_hosts(self):
        """
        should set env.all_hosts to its derived host list
        """
        hosts = ['a', 'b']
        roledefs = {'r1': ['c', 'd']}
        roles = ['r1']
        exclude_hosts = ['a']

        def command():
            self.assertEqual(set(env.all_hosts), set(['b', 'c', 'd']))
        task = Fake(callable=True, expect_call=True).calls(command)
        with settings(hide('everything'), roledefs=roledefs):
            execute(
                task, hosts=hosts, roles=roles, exclude_hosts=exclude_hosts
            )

    def test_should_print_executing_line_per_host(self):
        """
        should print "Executing" line once per host
        """
        state.output.running = True

        def task():
            pass
        execute(task, hosts=['host1', 'host2'])
        self.assertEqual(sys.stdout.getvalue(),
                         """[host1] Executing task 'task'
[host2] Executing task 'task'
""")

    def test_should_not_print_executing_line_for_singletons(self):
        """
        should not print "Executing" line for non-networked tasks
        """

        def task():
            pass
        with settings(hosts=[]):  # protect against really odd test bleed :(
            execute(task)
        self.assertEqual(sys.stdout.getvalue(), "")

    def test_should_return_dict_for_base_case(self):
        """
        Non-network-related tasks should return a dict w/ special key
        """
        def task():
            return "foo"
        self.assertEqual(execute(task), {'<local-only>': 'foo'})

    def test_should_return_dict_for_serial_use_case(self):
        """
        Networked but serial tasks should return per-host-string dict
        """
        ports = [2200, 2201]
        hosts = map(lambda x: '127.0.0.1:%s' % x, ports)

        @serial
        def task():
            return "foo"
        with hide('everything'):
            self.assertEqual(execute(task, hosts=hosts), {
                '127.0.0.1:2200': 'foo',
                '127.0.0.1:2201': 'foo'
            })

    @patch('fabric.operations._run_command')
    @patch('prestoadmin.fabric_patches.log_output')
    def test_should_preserve_None_for_non_returning_tasks(self, log_mock,
                                                          run_mock):
        """
        Tasks which don't return anything should still show up in the dict
        """
        def local_task():
            pass

        def remote_task():
            with hide('everything'):
                run_mock.return_value = 'hello'
                fabric.api.run('a command')
        self.assertEqual(execute(local_task), {'<local-only>': None})
        with hide('everything'):
            self.assertEqual(
                execute(remote_task, hosts=['host']),
                {'host': None}
            )

    def test_should_use_sentinel_for_tasks_that_errored(self):
        """
        Tasks which errored but didn't abort should contain an eg NetworkError
        """
        def task():
            fabric.api.run("whoops")
        host_string = 'localhost:1234'
        with settings(hide('everything'), skip_bad_hosts=True):
            retval = execute(task, hosts=[host_string])
        assert isinstance(retval[host_string], NetworkError)

    def test_parallel_return_values(self):
        """
        Parallel mode should still return values as in serial mode
        """
        @parallel
        @hosts('127.0.0.1:2200', '127.0.0.1:2201')
        def task():
            return env.host_string.split(':')[1]
        with hide('everything'):
            retval = execute(task)
        self.assertEqual(retval, {'127.0.0.1:2200': '2200',
                                  '127.0.0.1:2201': '2201'})

    @with_fakes
    def test_should_work_with_Task_subclasses(self):
        """
        should work for Task subclasses, not just WrappedCallableTask
        """
        class MyTask(Task):
            name = "mytask"
            run = Fake(callable=True, expect_call=True)
        mytask = MyTask()
        execute(mytask)

    @patch('prestoadmin.fabric_patches.error')
    def test_parallel_network_error(self, error_mock):
        """
        network error should call error
        """

        network_error = NetworkError('Network message')
        fabric.state.env.warn_only = False

        @parallel
        @hosts('127.0.0.1:2200', '127.0.0.1:2201')
        def task():
            raise network_error
        with hide('everything'):
            execute(task)
        error_mock.assert_called_with('Network message',
                                      exception=network_error.wrapped,
                                      func=fabric.utils.abort)

    @patch('prestoadmin.fabric_patches.error')
    def test_base_exception_error(self, error_mock):
        """
        base exception should call error
        """

        value_error = ValueError('error message')
        fabric.state.env.warn_only = True

        @parallel
        @hosts('127.0.0.1:2200', '127.0.0.1:2201')
        def task():
            raise value_error
        with hide('everything'):
            execute(task)
        # self.assertTrue(error_mock.is_called)
        args = error_mock.call_args
        self.assertEqual(args[0], ('error message',))
        self.assertEqual(type(args[1]['exception']), type(value_error))
        self.assertEqual(args[1]['exception'].args, value_error.args)

    def test_abort_should_not_raise_error(self):
        """
        base exception should call error
        """

        fabric.state.env.warn_only = False

        @parallel
        @hosts('127.0.0.1:2200', '127.0.0.1:2201')
        def task():
            fabric.utils.abort('aborting')
        with hide('everything'):
            execute(task)

    def test_abort_in_serial_should_not_raise_error(self):
        """
        base exception should call error
        """

        fabric.state.env.warn_only = False

        @serial
        @hosts('127.0.0.1:2200', '127.0.0.1:2201')
        def task():
            fabric.utils.abort('aborting')
        with hide('everything'):
            execute(task)

    def test_arg_exception_should_raise_error(self):
        @hosts('127.0.0.1:2200', '127.0.0.1:2201')
        def task(arg):
            pass
        with hide('everything'):
            self.assertRaisesRegexp(TypeError,
                                    'task\(\) takes exactly 1 argument'
                                    ' \(0 given\)', execute, task)
