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

from artemis.algorithms.dummyalgo import DummyAlgo1
import logging
import sys 

#logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)
#logging.debug('Logging configured in package init')


class AlgoTestCase(unittest.TestCase):
    
    def setUp(self):
        self.testalgo = DummyAlgo1("dummyalgo", myproperty='ptest')
        print(self.testalgo.__dict__)
        self.testalgo.initialize()
        print(self.testalgo.__dict__)
        print(self.testalgo.properties.myproperty)
    
    def tearDown(self):
        pass
    
    def test_algo(self):
        self.testalgo.execute('payload')
    
    def test_logger(self):
        self.testalgo.info('test info logger')
        # access logger through mangled attribute name
        self.testalgo._DummyAlgo1__logger.info('test info logger, again')
        self.testalgo._DummyAlgo1__logger.debug('test debug logger')


if __name__ == '__main__':
    unittest.main()
