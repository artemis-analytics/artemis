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
Example algorithm class.
"""
import sys
import logging
import random

from artemis.core.algo import AlgoBase


class DummyAlgo1(AlgoBase):

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.__logger.info('%s: __init__ DummyAlgo1' % self.name)

    def initialize(self):
        self.__logger.info('%s: initialize DummyAlgo1' % self.name)
        self.__logger.info('%s: property %s' %
                           (self.name, self.properties.myproperty))
        self.__logger.info('%s: Initialized DummyAlgo1' % self.name)

    def book(self):
        self.__timers = {}
        self._jp.hbook.book(self.name, "testh1", range(10))

    def execute(self, payload):
        if(logging.getLogger().isEnabledFor(logging.DEBUG) or
                self.__logger.isEnabledFor(logging.DEBUG)):

            # Prevent excessive formating calls when not required
            # Note that we can indepdently change the logging level
            # for algo loggers and root logger
            # Use string interpolation to prevent excessive format calls
            self.__logger.debug('%s: execute ' % self.name)
            # Check logging level if formatting requiered
            self.__logger.debug('{}: execute: payload {}'.
                                format(self.name, sys.getsizeof(payload)))
            self._jp.hbook.fill(self.name, "testh1", random.randint(0, 10))

    def finalize(self):
        pass
