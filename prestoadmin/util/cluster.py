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
Module for getting cluster information from YARN
"""

from contextlib import closing
import os

from fabric.state import env
import requests

from prestoadmin.util.exception import ConfigurationError
from prestoadmin.util.hadoop_conf import get_config
from prestoadmin.yarn_slider.config import HADOOP_CONF, DIR

YARN_RM_ADDRESS_KEY = 'yarn.resourcemanager.webapp.address'


def json_request(host, api_endpoint):
    url = 'http://' + host + '/' + api_endpoint
    headers = {'Accept': 'application/json'}
    with closing(requests.get(url, headers=headers)) as response:
        response.raise_for_status()
        return response.json()


def get_rm_host():
    """
    Get the address for the YARN resource manager webapp. First check the
    Apache Slider configuration in slider-client.xml, and then fall back to the
    hadoop configuration in yarn-site.xml. Note that the address should consist
    of both a hostname/IP address and a port. It is assumed that the address is
    valid.

    :return: The address of the YARN resource manager node.
    """
    slider_client_xml = os.path.join(
        env.conf[DIR], 'conf', 'slider-client.xml')
    try:
        slider_client_cfg = get_config(slider_client_xml)
        return slider_client_cfg[YARN_RM_ADDRESS_KEY]
    except:
        pass

    yarn_site_xml = os.path.join(env.conf[HADOOP_CONF], 'yarn-site.xml')
    try:
        yarn_site_cfg = get_config(yarn_site_xml)
        return yarn_site_cfg[YARN_RM_ADDRESS_KEY]
    except:
        raise ConfigurationError(
            'Failed to look up the ResourceManager webapp address (%s) in '
            '%s or %s.' %
            (YARN_RM_ADDRESS_KEY, slider_client_xml, yarn_site_xml))


def get_nodes_from_rm():
    nodes_response = json_request(
        get_rm_host(), '/'.join(['ws', 'v1', 'cluster', 'nodes']))

    nodelist = nodes_response['nodes']['node']
    hostnames = [n['nodeHostName'] for n in nodelist]
    return hostnames
