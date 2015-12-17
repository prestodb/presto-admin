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
from fabric.operations import put, sudo, local

from prestoadmin.yarn_slider.config import SliderConfig, \
    DIR, SLIDER_USER, APPNAME, JAVA_HOME, HADOOP_CONF, SLIDER_MASTER, \
    PRESTO_PACKAGE, SLIDER_CONFIG_DIR
from prestoadmin.util.base_config import requires_config

from prestoadmin.util.fabricapi import task_by_rolename

__all__ = ['install', 'uninstall']


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
        (get_slider_bin(conf), package_file, conf[APPNAME])

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
                             (get_slider_bin(conf), conf[APPNAME])
    run_slider(package_delete_command, conf)

    try:
        del conf[PRESTO_PACKAGE]
        conf.store_conf()
    except KeyError:
        pass

    local('rm %s' % (' '.join([os.path.join(SLIDER_CONFIG_DIR, f)
                     for f in SLIDER_PKG_DEFAULT_FILES])))
