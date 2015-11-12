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
from fabric.operations import sudo
from fabric.api import env

from prestoadmin.util import constants
import coordinator as coord
import prestoadmin.util.fabricapi as util
import workers as w

_LOGGER = logging.getLogger(__name__)


def coordinator():
    """
    Deploy the coordinator configuration to the coordinator node
    """
    if env.host in util.get_coordinator_role():
        _LOGGER.info("Setting coordinator configuration for " + env.host)
        configure_presto(coord.Coordinator().get_conf(),
                         constants.REMOTE_CONF_DIR)


def workers():
    """
    Deploy workers configuration to the worker nodes.
    This will not deploy configuration for a coordinator that is also a worker
    """
    if env.host in util.get_worker_role() and env.host \
            not in util.get_coordinator_role():
        _LOGGER.info("Setting worker configuration for " + env.host)
        configure_presto(w.Worker().get_conf(), constants.REMOTE_CONF_DIR)


def configure_presto(conf, remote_dir):
    print("Deploying configuration on: " + env.host)
    deploy(dict((name, output_format(content)) for (name, content)
                in conf.iteritems() if name != "node.properties"), remote_dir)
    deploy_node_properties(output_format(conf['node.properties']), remote_dir)


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


def deploy(confs, remote_dir):
    _LOGGER.info("Deploying configurations for " + str(confs.keys()))
    sudo("mkdir -p " + remote_dir)
    for name, content in confs.iteritems():
        write_to_remote_file(content, os.path.join(remote_dir, name))


def deploy_node_properties(content, remote_dir):
    _LOGGER.info("Deploying node.properties configuration")
    name = "node.properties"
    node_file_path = (os.path.join(remote_dir, name))
    node_id_command = (
        "if ! ( grep -q -s 'node.id' " + node_file_path + " ); then "
        "uuid=$(uuidgen); "
        "echo node.id=$uuid >> " + node_file_path + ";"
        "fi; "
        "sed -i '/node.id/!d' " + node_file_path + "; "
        )
    sudo(node_id_command)
    files.append(os.path.join(remote_dir, name), content, True)


def write_to_remote_file(text, filename):
    sudo("echo '%s' > %s" % (escape_single_quotes(text), filename))


def escape_single_quotes(text):
    # replace a single quote with a (closing) single quote followed by
    # an escaped quote followed by an (opening) single quote
    return text.replace(r"'", r"'\''")
