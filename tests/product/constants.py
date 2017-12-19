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

import prestoadmin
from prestoadmin import main_dir

BASE_IMAGES_TAG_CONFIG = 'base-images-tag.json'

_BASE_IMAGE_NAME = os.environ.get('BASE_IMAGE_NAME')
BASE_IMAGE_TAG = os.environ.get('BASE_IMAGE_TAG')

if _BASE_IMAGE_NAME is None:
    _BASE_IMAGE_NAME = 'prestodb/centos6-presto-admin-tests'

if BASE_IMAGE_TAG is None:
    try:
        with open(os.path.join(main_dir, BASE_IMAGES_TAG_CONFIG)) as tag_config:
            tag_json = json.load(tag_config)
        BASE_IMAGE_TAG = tag_json['base_images_tag']
    except KeyError:
        raise Exception("base_images_tag must be set in %s" % (BASE_IMAGES_TAG_CONFIG,))

BASE_IMAGE_NAME_BUILD = _BASE_IMAGE_NAME + "-build"
BASE_IMAGE_NAME_RUNTIME = _BASE_IMAGE_NAME + "-runtime"

print "using test build IMAGE %s:%s" % (BASE_IMAGE_NAME_BUILD, BASE_IMAGE_TAG)
print "using test runtime IMAGE %s:%s" % (BASE_IMAGE_NAME_RUNTIME, BASE_IMAGE_TAG)

LOCAL_RESOURCES_DIR = os.path.join(prestoadmin.main_dir,
                                   'tests/product/resources/')

DEFAULT_DOCKER_MOUNT_POINT = '/mnt/presto-admin'
DEFAULT_LOCAL_MOUNT_POINT = os.path.join(main_dir, 'tmp/docker-pa/')
