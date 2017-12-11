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
Abstract base class for bare image providers.

Bare image providers know how to bring bare docker images into existence for
the product tests.
"""

import abc

from docker import DockerClient


class BareImageProvider(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, tag_decoration):
        super(BareImageProvider, self).__init__()
        self.tag_decoration = tag_decoration

    @abc.abstractmethod
    def create_bare_images(self, cluster, master_name, slave_name):
        """Create master and slave images to be tagged with master_name and
        slave_name, respectively."""
        pass

    def get_tag_decoration(self):
        """Returns a string that's prepended to docker image tags for images
        based off of the bare image created by the provider."""
        return self.tag_decoration


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


class TagBareImageProvider(BareImageProvider):
    def __init__(
            self, base_master_name, base_slave_name, base_tag, tag_decoration):
        super(TagBareImageProvider, self).__init__(tag_decoration)
        self.base_master_name = base_master_name
        self.base_slave_name = base_slave_name
        self.base_tag = base_tag
        self.client = DockerClient()

    def create_bare_images(self, cluster, master_name, slave_name):
        self.client.images.pull(self.base_master_name, self.base_tag)
        self.client.images.pull(self.base_slave_name, self.base_tag)
        self.client.api.tag(self.base_master_name + ":" + self.base_tag, master_name)
        self.client.api.tag(self.base_slave_name + ":" + self.base_tag, slave_name)
