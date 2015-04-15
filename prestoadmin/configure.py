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
Common module for deploying the presto configuration
"""

import logging
import os

from fabric.contrib import files
from fabric.operations import put, sudo
from fabric.api import env, task
from prestoadmin.util import constants

import configuration as config
import coordinator as coord
import prestoadmin.util.fabricapi as util
import workers as w

__all__ = ["coordinator", "workers", "all"]
_LOGGER = logging.getLogger(__name__)


@task
def all():
    """
    Deploy configuration for all roles on the remote hosts
    """
    _LOGGER.info("Running configure all")
    coordinator()
    workers()


@task
def coordinator():
    """
    Deploy the coordinator configuration to the coordinator node
    """
    if env.host in util.get_coordinator_role():
        _LOGGER.info("Setting coordinator configuration for " + env.host)
        configure_presto(coord.get_conf(), constants.TMP_CONF_DIR,
                         constants.REMOTE_CONF_DIR)


@task
def workers():
    """
    Deploy workers configuration to the worker nodes.
    This will not deploy configuration for a coordinator that is also a worker
    """
    if env.host in util.get_worker_role() and env.host \
            not in util.get_coordinator_role():
        _LOGGER.info("Setting worker configuration for " + env.host)
        configure_presto(w.get_conf(), constants.TMP_CONF_DIR,
                         constants.REMOTE_CONF_DIR)


def configure_presto(conf, local_dir, remote_dir):
    write_conf_to_tmp(conf, local_dir)
    deploy([name for name in conf.keys() if name != "node.properties"],
           local_dir, remote_dir)
    deploy_node_properties(local_dir, remote_dir)


def write_conf_to_tmp(conf, conf_dir):
    _LOGGER.info("Writing configuration to temporary files.")
    for key, value in conf.iteritems():
        path = conf_dir + "/" + key
        config.write(output_format(value), path)


def output_format(conf):
    try:
        return dict_to_equal_format(conf)
    except AttributeError:
        pass
    try:
        return list_to_line_separated(conf)
    except TypeError:
        pass
    except AssertionError:
        pass
    return str(conf)


def dict_to_equal_format(conf):
    sorted_list = sorted(key_val_to_equal(conf.iteritems()))
    return list_to_line_separated(sorted_list)


def key_val_to_equal(items):
    return ["=".join(item) for item in items]


def list_to_line_separated(conf):
    assert not isinstance(conf, basestring)
    return "\n".join(conf)


def deploy(filenames, local_dir, remote_dir):
    _LOGGER.debug("Deploying configurations for " + str(filenames))
    sudo("mkdir -p " + remote_dir)
    for name in filenames:
        put(os.path.join(local_dir, name),
            os.path.join(remote_dir, name), True)


def deploy_node_properties(local_dir, remote_dir):
    _LOGGER.debug("Deploying node.properties configuration")
    name = "node.properties"
    node_file_path = (os.path.join(remote_dir, name))
    node_id_command = (
        "if ! ( grep -q 'node.id' " + node_file_path + " ); then "
        "uuid=$(uuidgen); "
        "echo node.id=$uuid >> " + node_file_path + ";"
        "fi; "
        "sed -i '/node.id/!d' " + node_file_path + "; "
        )
    sudo(node_id_command)
    with open(os.path.join(local_dir, name), 'r') as f:
        properties = f.read()
    files.append(os.path.join(remote_dir, name), properties, True)
