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
Creates bare images with HDP 2.3 installed for product tests that require
that hadoop is actually installed.
"""

from tests.bare_image_provider import TagBareImageProvider


class HdpBareImageProvider(TagBareImageProvider):
    def __init__(self):
        super(HdpBareImageProvider, self).__init__(
            'teradatalabs/hdp2.3-master', 'teradatalabs/hdp2.3-slave',
            'hdp2.3')
