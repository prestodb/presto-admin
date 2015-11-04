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
Provides bare images from existing tags in Docker. For some of the heftier
images, we don't want to go through a long and drawn-out Docker build on a
regular basis. For these, we count on having an image in Docker that we can
tag appropriately into the teradatalabs/pa_tests namespace. Test cleanup can
continue to obliterate that namespace without disrupting the actual heavyweight
images.

As an additional benefit, this means we can have tests depend on images that
the test code doesn't know how to build. That seems like a liability, but it
that the build process for complex images can be versioned outside of the
presto-admin codebase.
"""

from docker import Client

from tests.bare_image_provider import BareImageProvider


class TagBareImageProvider(BareImageProvider):
    def __init__(self, base_master_name, base_slave_name, tag_decoration):
        super(TagBareImageProvider, self).__init__()
        self.base_master_name = base_master_name
        self.base_slave_name = base_slave_name
        self.tag_decoration = tag_decoration
        self.client = Client()

    def create_bare_images(self, cluster, master_name, slave_name):
        self.client.tag(self.base_master_name, master_name)
        self.client.tag(self.base_slave_name, slave_name)

    def get_tag_decoration(self):
        return self.tag_decoration
