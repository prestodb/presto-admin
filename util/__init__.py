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

# Modules within util should only use the standard library because setup.py
# may rely on the modules. setup.py typically installs all dependencies.
# If a third party module is used, setup.py may attempt to import it while
# trying to install dependencies and an ImportError will be raised because
# the dependency has not been installed yet.
import os

main_dir = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))

with open(os.path.join(main_dir, 'prestoadmin/_version.py')) as version_file:
    __version__ = version_file.readlines()[-1].split()[-1].strip("\"'")
