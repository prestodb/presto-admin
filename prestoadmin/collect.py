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
This module contains the methods for gathering various debug information
for incident reporting using presto-admin
"""

import logging
import tarfile
import shutil

from fabric.contrib.files import exists
from fabric.operations import os, get
from fabric.tasks import execute
from fabric.api import env, runs_once, task
from fabric.utils import warn

from util.constants import REMOTE_PRESTO_LOG_DIR, PRESTOADMIN_LOG_DIR


TMP_PRESTO_DEBUG = '/tmp/presto-debug/'
OUTPUT_FILENAME = '/tmp/presto-debug-logs.tar.bz2'
PRESTOADMIN_LOG_NAME = 'presto-admin.log'
_LOGGER = logging.getLogger(__name__)

__all__ = ['logs']


@task
@runs_once
def logs():
    """
    Gather all the server logs and presto-admin log and create a tar file.
    """

    _LOGGER.debug("LOG directory to be archived: " + REMOTE_PRESTO_LOG_DIR)

    if not os.path.exists(TMP_PRESTO_DEBUG):
        os.mkdir(TMP_PRESTO_DEBUG)

    print 'Downloading logs from all the nodes'
    execute(file_get, REMOTE_PRESTO_LOG_DIR, TMP_PRESTO_DEBUG, roles=env.roles)

    copy_admin_log()

    make_tarfile(OUTPUT_FILENAME, TMP_PRESTO_DEBUG)
    print 'logs archive created : ' + OUTPUT_FILENAME


def copy_admin_log():
    shutil.copy(os.path.join(PRESTOADMIN_LOG_DIR, PRESTOADMIN_LOG_NAME),
                TMP_PRESTO_DEBUG)


def make_tarfile(output_filename, source_dir):
    tar = tarfile.open(output_filename, "w:bz2")

    try:
        tar.add(source_dir, arcname=os.path.basename(source_dir))
    finally:
        tar.close()


def file_get(remote_path, local_path):
    if exists(remote_path, True):
        get(remote_path, local_path + '%(host)s', True)
    else:
        warn("remote path " + remote_path + " not found on " + env.host)
