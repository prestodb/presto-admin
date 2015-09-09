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

""" Filesystem tools."""

import errno
import logging
import os


logger = logging.getLogger(__name__)


def ensure_parent_directories_exist(path):
    try:
        os.makedirs(os.path.dirname(path))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise e


def ensure_directory_exists(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise e


def write_to_file_if_not_exists(content, path):
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY

    try:
        os.makedirs(os.path.dirname(path))
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise

    try:
        file_handle = os.open(path, flags)
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise
    else:
        with os.fdopen(file_handle, 'w') as f:
            f.write(content)
