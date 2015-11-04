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
Abstract base class for installers.
"""

import abc


class BaseInstaller(object):
    __metaclass__ = abc.ABCMeta

    @staticmethod
    @abc.abstractmethod
    def get_dependencies():
        """Returns a list of installers that need to be run prior to running
        this one. Dependencies are considered satisfied if their
        assert_installed() returns without asserting.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def install(self):
        """Run the installer on the cluster.

        Installers may install something on one or more hosts of a cluster.
        After calling install(), the installer's assert_installed method should
        pass.
        """
        pass

    @abc.abstractmethod
    def get_keywords(self, *args, **kwargs):
        """Get a map of keyword: value mappings.

        We do a bunch of string formatting in the product tests when comparing
        actual command output to expected output. Installers can use this
        method to return additional keywords to be used in string formatting.
        """
        pass

    @staticmethod
    @abc.abstractmethod
    def assert_installed(testcase):
        """Check the cluster and assert if the installer hasn't been run. This
        should return without asserting if install() has been run.
        """
        raise NotImplementedError()
