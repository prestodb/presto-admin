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

from tests.bare_image_provider import TagBareImageProvider


class NoHadoopBareImageProvider(TagBareImageProvider):
    def __init__(self):
        super(NoHadoopBareImageProvider, self).__init__(
            'teradatalabs/centos6-ssh-oj8:6', 'teradatalabs/centos6-ssh-oj8:6',
            'nohadoop')
