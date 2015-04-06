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
This modules contains read-only constants used throughout
the presto admin project.
"""

import os

import prestoadmin

PRESTOADMIN_LOG = os.path.join(prestoadmin.main_dir, 'log')

# Logging Config File Locations
LOGGING_CONFIG_FILE_NAME = 'presto-admin-logging.ini'
LOGGING_CONFIG_FILE_DIRECTORIES = [
    os.path.join(prestoadmin.main_dir, 'prestoadmin')
]
