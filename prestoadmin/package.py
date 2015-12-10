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
Module for rpm package deploy and install using presto-admin
"""
import logging

from fabric.context_managers import settings, hide, shell_env
from fabric.decorators import task, runs_once
from fabric.operations import sudo, put, os, local
from fabric.state import env
from fabric.tasks import execute
from fabric.utils import abort

from prestoadmin.util import constants
from prestoadmin.standalone.config import StandaloneConfig
from prestoadmin.util.base_config import requires_config
from prestoadmin.util.fabricapi import get_host_list

_LOGGER = logging.getLogger(__name__)
__all__ = ['install']


@task
@runs_once
@requires_config(StandaloneConfig)
def install(local_path):
    """
    Install the rpm package on the cluster

    Args:
        local_path: Absolute path to the rpm to be installed
        --nodeps (optional): Flag to indicate if rpm install
            should ignore checking package dependencies. Equivalent
            to adding --nodeps flag to rpm -i.
    """
    check_if_valid_rpm(local_path)
    return execute(deploy_install, local_path, hosts=get_host_list())


def check_if_valid_rpm(local_path):
    _LOGGER.info("Checking rpm checksum to see if it is corrupted")
    with settings(hide('warnings', 'stdout'), warn_only=True):
        result = local('rpm -K --nosignature ' + local_path, capture=True)
    if 'MD5 NOT OK' in result.stdout:
        abort("Corrupted RPM. Try downloading the RPM again.")
    elif result.stderr:
        abort(result.stderr)


def deploy_install(local_path):
    deploy_action(local_path, rpm_install)


def deploy_upgrade(local_path):
    deploy_action(local_path, rpm_upgrade)


def deploy_action(local_path, rpm_action):
    deploy(local_path)
    rpm_action(os.path.basename(local_path))


def deploy(local_path=None):
    _LOGGER.info("Deploying rpm on %s..." % env.host)
    print("Deploying rpm on %s..." % env.host)
    sudo('mkdir -p ' + constants.REMOTE_PACKAGES_PATH)
    ret_list = put(local_path, constants.REMOTE_PACKAGES_PATH, use_sudo=True)
    if not ret_list.succeeded:
        _LOGGER.warn("Failure during put. Now using /tmp as temp dir...")
        ret_list = put(local_path, constants.REMOTE_PACKAGES_PATH,
                       use_sudo=True, temp_dir='/tmp')
    if ret_list.succeeded:
        print("Package deployed successfully on: " + env.host)


def rpm_install(rpm_name):
    _LOGGER.info("Installing the rpm")
    nodeps = ''
    if env.nodeps:
        nodeps = '--nodeps '

    if 'java8_home' not in env or env.java8_home is None:
        ret = sudo('rpm -i %s%s' %
                   (nodeps, os.path.join(constants.REMOTE_PACKAGES_PATH,
                                         rpm_name)))
    else:
        with shell_env(JAVA8_HOME='%s' % env.java8_home):
            ret = sudo('rpm -i %s%s' %
                       (nodeps, os.path.join(constants.REMOTE_PACKAGES_PATH,
                                             rpm_name)))
    if ret.succeeded:
        print("Package installed successfully on: " + env.host)


def rpm_upgrade(rpm_name):
    _LOGGER.info("Upgrading the rpm")
    nodeps = ''
    if env.nodeps:
        nodeps = '--nodeps '

    package_path = os.path.join(constants.REMOTE_PACKAGES_PATH, rpm_name)
    package_name = sudo('rpm -qp --queryformat \'%%{NAME}\' %s'
                        % package_path, quiet=True)

    ret_uninstall = sudo('rpm -e %s%s' % (nodeps, package_name))
    ret_install = sudo('rpm -i %s%s' % (nodeps, package_path))
    if ret_uninstall.succeeded and ret_install.succeeded:
        print("Package upgraded successfully on: " + env.host)
