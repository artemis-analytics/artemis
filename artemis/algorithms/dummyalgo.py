#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Testing algorithms
"""
import sys

from artemis.core.algo import AlgoBase
#from artemis.logger import Logger


class DummyAlgo1(AlgoBase):
   
    def __init__(self, name, **kwargs):
        self.__logger.info('Initialize Child')

        super().__init__(name, **kwargs)
        print('Child class dict')
        print(self.__dict__)
        self.info('{}: Initialized DummyAlgo1'.format(self.name))
    
    def initialize(self):
        pass

    def book(self):
        pass

    def execute(self, payload):
        print(self.__logger)
        print(self._DummyAlgo1__logger)
        self.__logger.info('Run: {} '.format(self.name))
        print('Input ', sys.getsizeof(payload))
        print('Test property', self.properties.myproperty)
        self.debug("Trying to debug")

    def finalize(self):
        pass

