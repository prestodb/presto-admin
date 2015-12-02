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

from prestoadmin.util.base_config import PresentFileConfig
from prestoadmin.yarn_slider.config import SLIDER_CONFIG_DIR

import os


class AppConfigJson(PresentFileConfig):
    def __init__(self):
        super(AppConfigJson, self).__init__(
            os.path.join(SLIDER_CONFIG_DIR, 'appConfig.json'))


class ResourcesJson(PresentFileConfig):
    def __init__(self):
        super(ResourcesJson, self).__init__(
            os.path.join(SLIDER_CONFIG_DIR, 'resources.json'))
