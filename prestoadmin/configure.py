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


import os

from fabric.contrib import files
from fabric.operations import put, sudo
from fabric.api import env, task

import configuration as config
import coordinator as coord
import prestoadmin
import prestoadmin.util.fabricapi as util
import workers as w

"""
Common module for deploying the presto configuration
"""

__all__ = ["coordinator", "workers", "all"]
REMOTE_DIR = "/etc/presto"
TMP_CONF_DIR = prestoadmin.main_dir + "/tmp/presto-conf"


@task
def all():
    """
    Deploy configuration for all roles on the remote hosts
    """
    coordinator()
    workers()


@task
def coordinator():
    """
    Deploy the coordinator configuration to the coordinator node
    """
    if env.host in util.get_coordinator_role():
        configure(coord.get_conf(), coord.TMP_OUTPUT_DIR)


@task
def workers():
    """
    Deploy workers configuration to the worker nodes.
    This will not deploy configuration for a coordinator that is also a worker
    """
    if env.host in util.get_worker_role() and env.host \
            not in util.get_coordinator_role():
        configure(w.get_conf(), w.TMP_OUTPUT_DIR)


def configure(conf, local_dir):
    write_conf_to_tmp(conf, local_dir)
    deploy(local_dir, REMOTE_DIR)


def write_conf_to_tmp(conf, conf_dir):
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


def deploy(local_dir, remote_dir):
    node_file = "node.properties"
    node_file_path = (os.path.join(remote_dir, node_file))
    node_id_command = (
        "if ! ( grep -q 'node.id' " + node_file_path + " ); then "
        "uuid=$(uuidgen); "
        "echo node.id=$uuid >> " + node_file_path + ";"
        "fi; "
        "sed -i '/node.id/!d' " + node_file_path + "; "
    )
    sudo(node_id_command)
    for name in os.listdir(local_dir):
        if name != node_file:
            put(os.path.join(local_dir, name),
                os.path.join(remote_dir, name), True)
        else:
            with open(os.path.join(local_dir, node_file), 'r') as f:
                properties = f.read()
            files.append(os.path.join(remote_dir, node_file), properties, True)
