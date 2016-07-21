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
Module for sending HTTP requests
"""
import urllib2


def send_get_request(url):
    response = None
    try:
        response = urllib2.urlopen(url)
        if response.getcode() != 200:
            exit('Get request to %s responded with status of %s' % (url, str(response.getcode())))
        else:
            headers = response.info()
            contents = response.read()
            return headers, contents
    finally:
        if response:
            response.close()


def send_authorized_post_request(url, data, authorization_string, content_type, content_length):
    response = None
    try:
        request = urllib2.Request(url, data,
                                  {'Content-Type': '%s' % content_type,
                                   'Content-Length': content_length,
                                   'Authorization': 'Basic %s' % authorization_string})
        response = urllib2.urlopen(request)
        status = response.getcode()
        headers = response.info()
        contents = response.read()
        if status != 201:
            print headers
            print contents
            exit('Failed to post to %s' % url)
    finally:
        if response:
            response.close()
