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
Module for doing transformations on dicts created from JSON.
A set of transformations is represented by a dict of
keypath -> tranformation-function pairs.
"""


def as_key(*components):
    """
    Since lists are not hashable, the components of a key path need to be
    represented as something that is hashable in the transformations map.
    This handles both lists of path components, and path components as and
    arbitrary number of args to the function.
    :param components: The path components to be represented as a key.
    :return: Something hashable that can be used as a dict key.
    """
    try:
        return '/'.join(components)
    except TypeError:
        return '/'.join(*components)


def _get_transform(kpath, transforms):
    return transforms.get(as_key(kpath))


def purge(kpath, value):
    """
    Returns a value that results in the keypath being removed from the JSON
    object.
    :param kpath: The keypath of the item to be purged
    :param value: The current value of the item to be purged
    :return: None
    """
    return None


def _transform(source, transformations, path):
    """
    Recursion on lists doesn't happen, which allows a value that is a list to
    be purged. This seems questionable and should probably be revisited if we
    actually need to manipulate a list and find that doing so this way proves
    to be cumbersome.
    """
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
    """
    Apply transformations to specific paths in the JSON object. Transformation
    functions receive two parameters: the keypath of the item they're being
    called to transform and that item's value in source.
    :param source: A JSON object
    :param transformations: A map of key path: transformation function entries.
    :return: The result of applying transformations to source.
    """
    return _transform(source, transformations, [])
