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
Provides bare images for standalone clusters.
"""

import re

from tests.bare_image_provider import TagBareImageProvider
from tests.product.constants import BASE_IMAGES_TAG
from tests.product.constants import BASE_IMAGE_NAME


class NoHadoopBareImageProvider(TagBareImageProvider):
    def __init__(self):
        # encode base image name into name of created test image, to prevent image name clash.
        decoration = 'nohadoop_' + re.sub(r"[^A-Za-z0-9]", "_", BASE_IMAGE_NAME)
        super(NoHadoopBareImageProvider, self).__init__(
            BASE_IMAGE_NAME, BASE_IMAGE_NAME,
            BASE_IMAGES_TAG, decoration)
