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
#


def to_boolean(string):
    """
    Parses the given string into a boolean.  If its already a boolean, its
    returned unchanged.

    This method does strict parsing; only the string "True" returns the boolean
    True, and only the string "False" returns the boolean False.  All other
    values throw a ValueError.

    :param string: the string to parse
    """
    if string is True or string == 'True':
        return True
    elif string is False or string == 'False':
        return False

    raise ValueError("invalid boolean string: %s" % string)
