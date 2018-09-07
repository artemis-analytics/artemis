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


if __name__ == '__main__':
    unittest.main()
