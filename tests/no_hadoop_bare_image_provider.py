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
from tests.product.constants import BASE_IMAGE_TAG
from tests.product.constants import BASE_IMAGE_NAME_BUILD
from tests.product.constants import BASE_IMAGE_NAME_RUNTIME


class NoHadoopBareImageProvider(TagBareImageProvider):
    def __init__(self, build_or_runtime="runtime"):
        if build_or_runtime == "runtime":
            base_image_name = BASE_IMAGE_NAME_RUNTIME
        elif build_or_runtime == "build":
            base_image_name = BASE_IMAGE_NAME_BUILD
        else:
            raise Exception("build_or_runtime must be one of \"build\" or \"runtime\"")

        # encode base image name into name of created test image, to prevent image name clash.
        decoration = 'nohadoop_' + re.sub(r"[^A-Za-z0-9]", "_", base_image_name)

        super(NoHadoopBareImageProvider, self).__init__(
            base_image_name, base_image_name,
            BASE_IMAGE_TAG, decoration)
