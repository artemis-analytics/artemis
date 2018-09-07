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


class DummyAlgo1(AlgoBase):
   
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
    
    def initialize(self):
        pass

    def book(self):
        pass

    def execute(self, payload):
        print('Run ', self.name)
        print('Input ', sys.getsizeof(payload))
        print('Test property', self.properties.myproperty)

    def finalize(self):
        pass

