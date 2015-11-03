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

from tests.bare_image_provider import BareImageProvider

from tests.product.constants import \
    BASE_IMAGE_NAME, BASE_IMAGE_TAG, BASE_TD_DOCKERFILE_DIR


class NoHadoopBareImageProvider(BareImageProvider):
    def __init__(self):
        super(NoHadoopBareImageProvider, self).__init__()

    def create_bare_images(self, cluster, master_name, slave_name):
        cluster.create_image(
            BASE_TD_DOCKERFILE_DIR,
            master_name,
            BASE_IMAGE_NAME,
            BASE_IMAGE_TAG
        )

        cluster.create_image(
            BASE_TD_DOCKERFILE_DIR,
            slave_name,
            BASE_IMAGE_NAME,
            BASE_IMAGE_TAG
        )

    def get_tag_decoration(self):
        return 'nohadoop'
