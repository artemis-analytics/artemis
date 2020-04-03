#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Property classes
"""

from collections import OrderedDict
from pprint import pformat

from artemis_format.pymodels.configuration_pb2 import Properties as Properties_pb


class Properties:
    """
    Dynamically create getter/setter for user-defined properties
    """

    def __init__(self, lock=False):
        self.lock = lock
        self.properties = dict()

    def __str__(self):
        return pformat(self.properties)

    def add_property(self, name, value):
        # Retain dictionary of properties
        self.properties[name] = value
        # Local fget and fset functions
        # lambdas defined directly in property below
        # fixes flake8 errors

        # add the property to self
        setattr(
            self.__class__,
            name,
            property(
                lambda self: self._get_property(name),
                lambda self, value: self._set_property(name, value),
            ),
        )
        # add corresponding local variable
        setattr(self, "_" + name, value)

    @staticmethod
    def from_msg(msg):
        """
        returns dict
        """
        _supported = {
            "str": str,
            "float": float,
            "int": int,
            "bool": bool,
            "dict": dict,
        }
        properties = {}
        for p in msg.property:
            if p.type == "NoneType":
                continue
            elif p.type == "dict" or p.type == "bool" or p.type == "list":
                properties[p.name] = eval(p.value)
            else:
                properties[p.name] = _supported[p.type](p.value)
        return properties

    def to_dict(self):
        """
        Ordered dictionary of all user-defined properties
        """
        _dict = OrderedDict()
        for key in self.properties:
            _dict[key] = self.properties[key]
        return _dict

    def to_msg(self):
        message = Properties_pb()
        for key in self.properties:
            if self.properties[key] is None:
                continue
            pbuf = message.property.add()
            pbuf.name = key
            pbuf.type = type(self.properties[key]).__name__
            pbuf.value = str(self.properties[key])
        return message

    def _set_property(self, name, value):
        if not self.lock:
            setattr(self, "_" + name, value)
        else:
            print('Cannot change "{}": properties are locked'.format(name))

    def _get_property(self, name):
        return getattr(self, "_" + name)
