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
Following taken from faker/utils/loading
"""
import os
import sys
import pkgutil


def get_path(module):
    if getattr(sys, "frozen", False):
        path = os.path.dirname(sys.executable)
    else:
        path = os.path.dirname(os.path.realpath(module.__file__))
    return path


def list_module(module):
    path = get_path(module)
    modules = [name for finder, name, is_pkg in pkgutil.iter_modules([path]) if is_pkg]
    return modules


def find_available_providers(modules):
    available_providers = set()
    for prvdrs_mod in modules:
        prvdrs = [
            ".".join([prvdrs_mod.__package__, mod])
            for mod in list_module(prvdrs_mod)
            if mod != "__pycache__"
        ]
    available_providers.update(prvdrs)
    return sorted(available_providers)
