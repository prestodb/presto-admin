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
Module for managing presto/YARN integration.
"""

from contextlib import contextmanager
import os.path

from fabric.api import env, task, abort
from fabric.context_managers import shell_env, quiet, warn_only
from fabric.operations import put, sudo, local, run
from fabric.utils import puts

from prestoadmin.fabric_patches import execute
from prestoadmin.util.cluster import get_nodes_from_rm
from prestoadmin.yarn_slider.slider_application_configs import AppConfigJson, \
    ResourcesJson
from prestoadmin.yarn_slider.slider_exit_codes import EXIT_SUCCESS, \
    EXIT_UNKNOWN_INSTANCE
from prestoadmin.yarn_slider.config import SliderConfig, \
    DIR, SLIDER_USER, APP_INST_NAME, JAVA_HOME, HADOOP_CONF, SLIDER_MASTER, \
    PRESTO_PACKAGE, SLIDER_CONFIG_DIR, SLIDER_PKG_NAME
from prestoadmin.util.base_config import requires_config

from prestoadmin.util.fabricapi import task_by_rolename

__all__ = ['install', 'uninstall', 'start', 'stop', 'create', 'build',
           'destroy']


SLIDER_PKG_DEFAULT_FILES = ['appConfig-default.json', 'resources-default.json']


def get_slider_bin(conf):
    return os.path.join(conf[DIR], 'bin', 'slider')


def run_slider(slider_command, conf):
    with shell_env(JAVA_HOME=conf[JAVA_HOME],
                   HADOOP_CONF_DIR=conf[HADOOP_CONF]):
        return sudo(slider_command, user=conf[SLIDER_USER])


@task
@requires_config(SliderConfig)
@task_by_rolename(SLIDER_MASTER)
def install(presto_yarn_package):
    """
    Install the presto-yarn package on the cluster using Apache Slider. The
    presto-yarn package takes the form of a zip file that conforms to Slider's
    packaging requirements. After installing the presto-yarn package the presto
    application is registered with Slider.

    Before Slider can install the presto-yarn package, the slider user's hdfs
    home directory needs to be created. This needs to be done by a user that
    has write access to the hdfs /user directory, typically the user hdfs or a
    member of the superuser group.

    The name of the presto application is arbitrary and set in the slider
    configuration file. The default is PRESTO

    :param presto_yarn_package: The zip file containing the presto-yarn
                                package as structured for Slider.
    """
    conf = env.conf
    package_filename = os.path.basename(presto_yarn_package)
    package_file = os.path.join('/tmp', package_filename)

    result = put(presto_yarn_package, package_file)
    if result.failed:
        abort('Failed to send slider application package to %s on host %s' %
              (package_file, env.host))

    package_install_command = \
        '%s package --install --package %s --name %s' % \
        (get_slider_bin(conf), package_file, SLIDER_PKG_NAME)

    try:
        run_slider(package_install_command, conf)

        conf[PRESTO_PACKAGE] = package_filename
        conf.store_conf()

        local('unzip %s %s -d %s' %
              (presto_yarn_package, ' '.join(SLIDER_PKG_DEFAULT_FILES),
               SLIDER_CONFIG_DIR))
    finally:
        sudo('rm -f %s' % (package_file))


@task
@requires_config(SliderConfig)
@task_by_rolename(SLIDER_MASTER)
def uninstall():
    """
    Uninstall unregisters the presto application with slider and removes the
    installed package.
    """
    conf = env.conf
    package_delete_command = '%s package --delete --name %s' % \
                             (get_slider_bin(conf), SLIDER_PKG_NAME)
    run_slider(package_delete_command, conf)

    try:
        del conf[PRESTO_PACKAGE]
        conf.store_conf()
    except KeyError:
        pass

    with quiet():
        local('rm %s' % (' '.join([os.path.join(SLIDER_CONFIG_DIR, f)
                         for f in SLIDER_PKG_DEFAULT_FILES])))


@contextmanager
def remote_temp_file(local_path, remote_path):
    result = []
    try:
        result = put(local_path, remote_path)
        if len(result) == 1:
            yield result[0]
        else:
            yield result
    finally:
        for remote_path in result:
            run('/bin/rm "%s"' % remote_path)


def start_app():
    hostnames = get_nodes_from_rm()
    execute(mk_data_dir, hosts=hostnames)
    start_cmd = (
        '%s start %s' % (get_slider_bin(env.conf), env.conf[APP_INST_NAME]))
    with warn_only():
        return run_slider(start_cmd, env.conf)


BUILD = 'build'
CREATE = 'create'


def _build_or_create(command=BUILD):
    appConfig = AppConfigJson()
    resources = ResourcesJson()

    with remote_temp_file(appConfig.get_config_path(), '/tmp') as \
            remote_appConfig:
        with remote_temp_file(resources.get_config_path(), '/tmp') as \
                remote_resources:
            slider_cmd = '%s %s %s --template %s --resources %s' % (
                get_slider_bin(env.conf), command, env.conf[APP_INST_NAME],
                remote_appConfig, remote_resources)
            return run_slider(slider_cmd, env.conf)


@task
@requires_config(SliderConfig, AppConfigJson, ResourcesJson)
@task_by_rolename(SLIDER_MASTER)
def create():
    """
    Build and start the configured Presto application instance on the cluster
    using Apache Slider.
    """
    return _build_or_create(command=CREATE)


@task
@requires_config(SliderConfig, AppConfigJson, ResourcesJson)
@task_by_rolename(SLIDER_MASTER)
def build():
    """
    Build, but do NOT start the configured Presto application instance on the
    cluster using Apache Slider. The application instance can be started by
    invoking the presto-admin command to start the application instance.
    """
    return _build_or_create(command=BUILD)


# Starting Presto doesn't actually require AppConfig and Resources, but if the
# app doesn't exist and we have to fall back to creating it, it's too late to
# prompt the user for anything we don't have a default for when we call
# create().
@task
@requires_config(SliderConfig, AppConfigJson, ResourcesJson)
@task_by_rolename(SLIDER_MASTER)
def start():
    """
    Start the configured Presto application instance. If the application
    instance does not yet exist, create it and start it.
    """
    appname = env.conf[APP_INST_NAME]
    start_result = start_app()
    if start_result.return_code == EXIT_SUCCESS:
        return start_result
    elif start_result.return_code == EXIT_UNKNOWN_INSTANCE:
        puts('Creating and starting presto application instance %s' %
             (appname,))
        return create()
    else:
        abort('Failed to start presto application instance %s:' %
              (appname,))


@task
@requires_config(SliderConfig)
@task_by_rolename(SLIDER_MASTER)
def stop():
    """
    Stop the configured Presto application instance.
    """
    stop_command = '%s stop %s' % (
        get_slider_bin(env.conf), env.conf[APP_INST_NAME])
    return run_slider(stop_command, env.conf)


@task
@requires_config(SliderConfig)
@task_by_rolename(SLIDER_MASTER)
def destroy():
    """
    Destroy the Presto application instance. Local configuration files will be
    preserved, and the application instance will be removed from Apache Slider.
    """
    destroy_command = \
        '%s destroy %s' % (get_slider_bin(env.conf), env.conf[APP_INST_NAME])
    return run_slider(destroy_command, env.conf)


def mk_data_dir():
    app_config = AppConfigJson()
    command = \
        '[ -d %(dir)s ] || mkdir -p %(dir)s && ' \
        'chown %(user)s:%(group)s %(dir)s' % (
            {'dir': app_config.get_data_dir(),
             'user': app_config.get_user(),
             'group': app_config.get_group()})
    return sudo(command)
