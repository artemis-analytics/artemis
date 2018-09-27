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
import json

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
        self.mydummy2 = self.DummyClass(a_property='dynamic')

    def tearDown(self):
        pass

    def test_dummy(self):
        assert(self.mydummy.static_prop == 'static')
    
    def test_property(self):
        
        print(self.mydummy2.__dict__)
        assert(self.mydummy2.properties.a_property == 'dynamic')
    
    def test_dict(self):
        _props = self.mydummy2.properties.to_dict()
        print(_props)
        #json.dumps(_props, indent=4)





if __name__ == '__main__':
    unittest.main()

