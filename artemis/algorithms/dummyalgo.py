#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented 
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""
Testing algorithms
"""
import sys
import logging
import random

from artemis.core.algo import AlgoBase
from artemis.core.physt_wrapper import Physt_Wrapper


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
        self.hbook = Physt_Wrapper()
        self.hbook.book(self.name, "testh1", range(10))

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
            self.hbook.fill(self.name, "testh1", random.randint(0, 10))

    def finalize(self):
        pass
