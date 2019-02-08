#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented 
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""

"""
import unittest

from artemis.core.algo import AlgoBase
from artemis.algorithms.dummyalgo import DummyAlgo1
import logging
from pprint import pformat
import sys
from google.protobuf import text_format

from artemis.decorators import timethis

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
        
        @timethis
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
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
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
        print('Timing--------------')
        print(self.testalgo.execute(b'payload')[-1])
        print('End-------------')
    
    def test_logger(self):
        # access logger through mangled attribute name
        self.testalgo._TestAlgo__logger.info('test info logger, again')
        self.testalgo._TestAlgo__logger.debug('test debug logger')
        self.testalgo.logger.info("Use logger getter property")

    def test_dict(self):
       print(pformat(self.testalgo.to_dict()))

    def test_msg(self):
        dummy = DummyAlgo1('dummy', prop=3.0)
        msg = dummy.to_msg()
        print(text_format.MessageToString(msg))        
        a_algo = AlgoBase.from_msg(self.testalgo.logger, msg)
        print(a_algo.__dict__)


if __name__ == '__main__':
    unittest.main()
