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
Module defining constants global to the product tests
"""

import json
import os
import sys

import prestoadmin
from prestoadmin import main_dir

BASE_IMAGES_TAG_CONFIG = 'base-images-tag.json'

try:
    with open(os.path.join(main_dir, BASE_IMAGES_TAG_CONFIG)) as bit_conf:
        bit_json = json.load(bit_conf)
    BASE_IMAGES_TAG = bit_json['base_images_tag']
except KeyError:
    print "BASE_IMAGES_TAG must be set in %s" % (BASE_IMAGES_TAG_CONFIG,)
    sys.exit(1)


JAVA_VERSION_DIR = 'jdk1.8.0_92'
DEFAULT_JAVA_DIR = os.path.join('/usr', 'java', JAVA_VERSION_DIR)

LOCAL_RESOURCES_DIR = os.path.join(prestoadmin.main_dir,
                                   'tests/product/resources/')

DEFAULT_DOCKER_MOUNT_POINT = '/mnt/presto-admin'
DEFAULT_LOCAL_MOUNT_POINT = os.path.join(main_dir, 'tmp/docker-pa/')
