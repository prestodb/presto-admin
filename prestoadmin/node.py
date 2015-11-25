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
Module for the presto coordinator's configuration.
Loads and validates the coordinator.json file and creates the files needed
to deploy on the presto cluster
"""
from abc import abstractmethod, ABCMeta
import logging

import config
import presto_conf
from prestoadmin.presto_conf import get_presto_conf

_LOGGER = logging.getLogger(__name__)


class Node:
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    def get_conf(self):
        conf = get_presto_conf(self._get_conf_dir())
        for name in presto_conf.REQUIRED_FILES:
            if name not in conf:
                _LOGGER.debug('%s configuration for %s not found.  '
                              'Default configuration will be deployed',
                              type(self).__name__, name)

        defaults = self.build_defaults()
        config.fill_defaults(conf, defaults)
        self.validate(conf)
        return conf

    @abstractmethod
    def _get_conf_dir(self):
        pass

    @abstractmethod
    def build_defaults(self):
        pass

    @staticmethod
    @abstractmethod
    def validate(conf):
        pass
