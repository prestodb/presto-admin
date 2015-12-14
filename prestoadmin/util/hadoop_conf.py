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
    """
    Parse an XML file that conforms to Hadoop's conventions for a configuration
    file. The returned dictionary will have one entry for each property element
    in the XML file. The key will be the text of the name element; the value
    will be the text of the value element.

    Error checking is minimal and limited to what is required to ensure that we
    return a sane dictionary. The assumption here is that the configuration
    file needs to be both well-formed and valid in order for it to be useful to
    Hadoop. Accordingly, the error checking is not as full-featured as it might
    be. Users are expected to have ensured that configuration errors have been
    fixed as part of making Hadoop run.

    :param config_path: The location to look for a Hadoop configuration file.
    :return: A dictionary of the key -> value pairs in the Hadoop configuration
             file.
    """
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
