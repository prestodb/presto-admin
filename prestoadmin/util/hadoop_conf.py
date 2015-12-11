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
Module to parse hadoop config files into a dictionary.
"""

from prestoadmin.util.exception import ConfigurationError

from xml.etree import ElementTree

def get_config(config_path):
    result = None
    key = None
    value = None
    context = ElementTree.iterparse(config_path, events=('start', 'end'))
    for event, element in context:
        tag = element.tag

        if event == 'start':
            if tag == 'configuration':
                result = {}
            elif tag == 'property':
                if key is not None or value is not None:
                    raise ConfigurationError(
                        'Non-empty name or value at start of property tag in '
                        'file %s: name="%s", value="%s"' %
                        (config_path, key, value))
            continue

        if event == 'end':
            content = element.text
            if content:
                content = content.encode('utf-8').strip()

            if tag == 'name':
                key = content
            elif tag == 'value':
                value = content
            elif tag == 'property':
                if key and value:
                    result[key] = value
                else:
                    raise ConfigurationError(
                        'Incomplete property in file %s: name="%s", value="%s"'
                        % (config_path, key, value))
                key = None
                value = None
            elif tag == 'configuration':
                return result
    # Missing closing configuation tag raises a ParseError






