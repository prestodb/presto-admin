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


class BareImageProvider:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def create_bare_images(self, cluster, master_name, slave_name):
        """Create master and slave images to be tagged with master_name and
        slave_name, respectivesly."""
        pass

    @abc.abstractmethod
    def get_tag_decoration(self):
        """Returns a string that's prepended to docker image tags for images
        based off of the bare image created by the provider."""
        pass
