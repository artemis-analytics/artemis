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
Various decorator methods
"""
import time
from functools import wraps


def timethis(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        r = func(*args, **kwargs)
        end = time.perf_counter()
        mytime = (end - start) / 1e-3
        return r, mytime

    return wrapper


def iterable(cls):
    """
    Generate a dictionary from class properties
    Used to encapulate default values for configurable class properties

    Stack Overflow "proper way to use kwargs in python"
    """

    def iterfn(self):
        iters = dict((x, y) for x, y in cls.__dict__.items() if x[:2] != "__")
        iters.update(self.__dict__)

        for x, y in iters.items():
            yield x, y

    cls.__iter__ = iterfn
    return cls
