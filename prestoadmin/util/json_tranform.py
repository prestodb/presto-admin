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


def as_key(*components):
    try:
        return '/'.join(components)
    except TypeError:
        return '/'.join(*components)


def _get_transform(kpath, transforms):
    return transforms.get(as_key(kpath))


def purge(kpath, value):
    return None


def _transform(source, transformations, path):
    if type(source) is dict:
        result = {}
        for k in source:
            kpath = path[:]
            kpath.append(k)
            transformation = _get_transform(kpath, transformations)
            if transformation:
                tvalue = transformation(kpath, source[k])
                if tvalue is not None:
                    result[k] = tvalue
            else:
                result[k] = _transform(source[k], transformations, kpath)
        return result
    else:
        return source


def transform(source, transformations):
    return _transform(source, transformations, [])
