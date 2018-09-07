#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Dynamic property creation testing suite
"""
import unittest

from artemis.core.properties import Properties


class PropertyTestCase(unittest.TestCase):
    
    class DummyClass():

        def __init__(self, **kwargs):
            self.static_prop = 'static'
            self.properties = Properties()
            for key in kwargs:
                print(key, kwargs[key])
                self.properties.add_property(key, kwargs[key])
     
    def setUp(self):
        
        self.mydummy = self.DummyClass()
    
    def tearDown(self):
        pass

    def test_dummy(self):
        assert(self.mydummy.static_prop == 'static')
    
    def test_property(self):
        self.mydummy2 = self.DummyClass(a_property='dynamic')
        print(self.mydummy2.__dict__)
        assert(self.mydummy2.a_property == 'dynamic')



if __name__ == '__main__':
    unittest.main()

