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
from fabric.context_managers import shell_env
from fabric.decorators import runs_once
from fabric.operations import put, sudo, local
from fabric.tasks import execute

from prestoadmin.slider.config import requires_conf, store_conf, \
    DIR, SLIDER_USER, APPNAME, JAVA_HOME, HADOOP_CONF, SLIDER_MASTER, \
    PRESTO_PACKAGE, SLIDER_CONFIG_PATH, SLIDER_CONFIG_DIR

from prestoadmin.util.fabricapi import get_host_list, task_by_rolename

__all__ = ['slider_install', 'slider_uninstall', 'install', 'uninstall']


SLIDER_PKG_DEFAULT_FILES = ['appConfig-default.json', 'resources-default.json']


@task
@requires_conf
@runs_once
def slider_install(slider_tarball):
    """
    Install slider on the slider master. You must provide a tar file on the
    local machine that contains the slider distribution.

    :param slider_tarball: The gzipped tar file containing the Apache slider
    distribution
    """
    execute(deploy_install, slider_tarball, hosts=get_host_list())


def deploy_install(slider_tarball):
    slider_dir = env.conf[DIR]
    slider_parent = os.path.dirname(slider_dir)
    slider_file = os.path.join(slider_parent, os.path.basename(slider_tarball))

    sudo('mkdir -p %s' % (slider_dir))

    result = put(slider_tarball, os.path.join(slider_parent, slider_file))
    if result.failed:
        abort('Failed to send slider tarball %s to directory %s on host %s' %
              (slider_tarball, slider_dir, env.host))

    sudo('gunzip -c %s | tar -x -C %s --strip-components=1 && rm -f %s' %
         (slider_file, slider_dir, slider_file))


@task
@requires_conf
@task_by_rolename(SLIDER_MASTER)
def slider_uninstall():
    """
    Uninstall slider from the slider master.
    """
    sudo('rm -r "%s"' % (env.conf[DIR]))


def get_slider_bin(conf):
    return os.path.join(conf[DIR], 'bin', 'slider')


def run_slider(slider_command, conf):
    with shell_env(JAVA_HOME=conf[JAVA_HOME],
                   HADOOP_CONF_DIR=conf[HADOOP_CONF]):
        return sudo(slider_command, user=conf[SLIDER_USER])


@task
@requires_conf
@task_by_rolename(SLIDER_MASTER)
def install(presto_yarn_package):
    """
    Install the presto-yarn package on the cluster using Apache Slider. The
    presto-yarn package takes the form of a zip file that conforms to Slider's
    packaging requirements. After installing the presto-yarn package the presto
    application is registered with Slider.

    The name of the presto application is arbitrary and set in the slider
    configuration file. The default is PRESTO

    :param presto_yarn_package: The zip file containing the presto-yarn
    package as structured for slider.
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
        (get_slider_bin(conf), package_file, conf[APPNAME])

    try:
        run_slider(package_install_command, conf)

        env.conf[PRESTO_PACKAGE] = package_filename
        store_conf(env.conf, SLIDER_CONFIG_PATH)

        local('unzip %s %s -d %s' %
              (presto_yarn_package, ' '.join(SLIDER_PKG_DEFAULT_FILES),
               SLIDER_CONFIG_DIR))
    finally:
        sudo('rm -f %s' % (package_file))


@task
@requires_conf
@task_by_rolename(SLIDER_MASTER)
def uninstall():
    """
    Uninstall unregisters the presto application with slider and removes the
    installed package.
    """
    conf = env.conf
    package_delete_command = '%s package --delete --name %s' % \
                             (get_slider_bin(conf), conf[APPNAME])
    run_slider(package_delete_command, conf)

    try:
        del env.conf[PRESTO_PACKAGE]
        store_conf(env.conf, SLIDER_CONFIG_PATH)
    except KeyError:
        pass

    local('rm %s' % (' '.join([os.path.join(SLIDER_CONFIG_DIR, f)
                     for f in SLIDER_PKG_DEFAULT_FILES])))
