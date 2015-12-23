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
Utility functions for dealing with slider
"""


def degarbage_json(garbage_output):
    nest_level = 0
    json_begin = None
    json_end = None
    for i in xrange(0, len(garbage_output)):
        c = garbage_output[i]
        if c == '{':
            if nest_level == 0:
                json_begin = i
            nest_level += 1

        if c == '}':
            nest_level -= 1
            if nest_level == 0:
                json_end = i + 1
                break

    if json_begin is not None and json_end:
        return garbage_output[json_begin:json_end]

    return None
