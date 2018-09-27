#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""

"""
import unittest

from artemis.core.algo import AlgoBase
import logging
from pprint import pformat
import sys 

logging.getLogger().setLevel(logging.DEBUG)

class AlgoTestCase(unittest.TestCase):
    class TestAlgo(AlgoBase):
       
        def __init__(self, name, **kwargs):
            super().__init__(name, **kwargs)
            self.__logger.debug(pformat(kwargs))
            self.__logger.debug(pformat(self.__dict__))
            self.__logger.debug(pformat(self.__class__.__dict__))
            self.__logger.info('%s: Initialized DummyAlgo1' % self.name)
        
        def initialize(self):
            self.__logger.info(self.__logger)
            self.__logger.info(self._TestAlgo__logger)
            self.__logger.info('%s: property %s' % (self.name, self.properties.myproperty))

        def book(self):
            pass

        def execute(self, payload):
            if(logging.getLogger().isEnabledFor(logging.DEBUG) or
                    self.__logger.isEnabledFor(logging.DEBUG)):

                # Prevent excessive formating calls when not required
                # Note that we can indepdently change the logging level 
                # for algo loggers and root logger
                # Use string interpolation to prevent excessive format calls
                self.__logger.debug('%s: execute ' % self.name)
                # Check logging level if formatting requiered
                self.__logger.debug('{}: execute: payload {}'.format(self.name, sys.getsizeof(payload)))
            
            self.__logger.debug("Trying to debug")

        def finalize(self):
            pass
    
    def setUp(self):
        self.testalgo = self.TestAlgo("testalgo", myproperty='ptest', loglevel='DEBUG')
        print("Name", self.testalgo.name)
        self.testalgo2 = self.TestAlgo("testalgo2", myproperty='ptest', loglevel='DEBUG')
        print("Name", self.testalgo2.name)
        print(self.testalgo.__dict__)
        self.testalgo.initialize()
        print("Name", self.testalgo.name)
        print(self.testalgo.__dict__)
        print(self.testalgo.properties.myproperty)
    
    def tearDown(self):
        pass
    
    def test_algo(self):
        self.testalgo.execute('payload')
    
    def test_logger(self):
        # access logger through mangled attribute name
        self.testalgo._TestAlgo__logger.info('test info logger, again')
        self.testalgo._TestAlgo__logger.debug('test debug logger')
        self.testalgo.logger.info("Use logger getter property")

    def test_dict(self):
       print(pformat(self.testalgo.to_dict())) 


if __name__ == '__main__':
    unittest.main()
