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

import os.path

from fabric.api import env, task, abort
from fabric.context_managers import shell_env, settings
from fabric.decorators import runs_once
from fabric.operations import put, sudo, local
from fabric.tasks import execute

from prestoadmin import config as conf

SLIDER_PKG_DEFAULT_DEST = '/etc/opt/prestoadmin/slider'
from prestoadmin.slider.appConfig import DEFAULT_CONF_FILENAME \
    as APPCONFIG_DEFAULT
from prestoadmin.slider.appConfig import copy_default

from prestoadmin.slider.config import requires_conf, store_conf, \
    DIR, SLIDER_USER, APPNAME, JAVA_HOME, HADOOP_CONF, SLIDER_MASTER, \
    INSTANCE_NAME, PRESTO_PACKAGE, SLIDER_CONFIG_PATH

__all__ = ['slider_install', 'slider_uninstall', 'install', 'uninstall',
           'status', 'test']


SLIDER_PKG_DEFAULT_FILES = [APPCONFIG_DEFAULT, 'resources-default.json']

from prestoadmin.util.fabricapi import get_host_list, task_by_rolename


@task
@requires_conf
@runs_once
def slider_install(slider_tarball):
    """
    Install slider on the slider master. You must provide a tar file on the
    local machine that contains the slider distribution.

    :param slider_tarball:
    """
    execute(deploy_install, slider_tarball, hosts=get_host_list())


def deploy_install(slider_tarball):
    slider_dir = env.conf[DIR]
    slider_parent = os.path.dirname(slider_dir)
    slider_file = os.path.join(slider_parent, os.path.basename(slider_tarball))

    sudo('mkdir -p %s' % (slider_dir))

    result = put(slider_tarball, os.path.join(slider_parent, slider_file))
    if result.failed:
        abort('Failed to send slider tarball to directory %s on host %s' %
              (slider_tarball, slider_dir, env.host))

    sudo('gunzip -c %s | tar -x -C %s --strip-components=1 && rm -f %s' %
         (slider_file, slider_dir, slider_file))


@task
@requires_conf
@task_by_rolename(SLIDER_MASTER)
def slider_uninstall():
    sudo('rm -r "%s"' % (env.conf[DIR]))


def get_slider_bin():
    return os.path.join(env.conf[DIR], 'bin', 'slider')


def run_slider(slider_command, conf):
    with shell_env(JAVA_HOME=conf[JAVA_HOME],
                   HADOOP_CONF_DIR=conf[HADOOP_CONF]):
        sudo(slider_command, user=conf[SLIDER_USER])

@task
@requires_conf
@task_by_rolename(SLIDER_MASTER)
def status():
    conf = env.conf
    slider_command = '%s status %s' % (get_slider_bin(), conf[INSTANCE_NAME])
    run_slider(slider_command, conf)


@task
@requires_conf
@task_by_rolename(SLIDER_MASTER)
def install(presto_slider_package):
    conf = env.conf
    package_filename = os.path.basename(presto_slider_package)
    package_file = os.path.join('/tmp', package_filename)

    result = put(presto_slider_package, package_file)
    if result.failed:
        abort('Failed to send slider application package to %s on host %s' %
              (package_file, env.host))

    slider_command = '%s package --install --package %s --name %s' % \
                     (get_slider_bin(), package_file, conf[APPNAME])

    try:
        run_slider(slider_command, conf)

        env.conf[PRESTO_PACKAGE] = package_filename
        store_conf(env.conf, SLIDER_CONFIG_PATH)

        local('unzip %s %s -d %s' %
              (presto_slider_package, ' '.join(SLIDER_PKG_DEFAULT_FILES),
               SLIDER_PKG_DEFAULT_DEST))
    finally:
        sudo('rm -f %s' % (package_file))


@task
@requires_conf
@task_by_rolename(SLIDER_MASTER)
def uninstall():
    conf = env.conf
    slider_command = '%s package --delete --name %s' % \
                     (get_slider_bin(), conf[APPNAME])
    run_slider(slider_command, conf)

    try:
        del env.conf[PRESTO_PACKAGE]
        store_conf(env.conf, SLIDER_CONFIG_PATH)
    except KeyError:
        pass

    local('rm %s' % (' '.join([os.path.join(SLIDER_PKG_DEFAULT_DEST, f)
                            for f in SLIDER_PKG_DEFAULT_FILES])))


@task
@requires_conf
@runs_once
def test(app_config_path):
    with settings(parallel=False):
        app_config = conf.get_conf_from_json_file(app_config_path)
        print app_config
        copy = copy_default(app_config, [])
        print
        print copy


@task
@requires_conf
@task_by_rolename(SLIDER_MASTER)
def start():
    conf = env.conf
    slider_command = '%s start %s'
