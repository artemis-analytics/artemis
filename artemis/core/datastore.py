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
Arrow object store for managing access to data associated with nodes in a process tree.
"""

from .singleton import Singleton


class ArrowSets(metaclass=Singleton):
    def __init__(self):
        self.arrow_dict = {}

    def add_to_dict(self, key, batch):
        self.arrow_dict[key] = batch

    def get_data(self, key):
        return self.arrow_dict[key]

    def book(self, key):
        self.arrow_dict[key] = None

    def contains(self, key):
        return key in self.arrow_dict

    def is_empty(self):
        if not bool(self.arrow_dict):
            return True
        return False
