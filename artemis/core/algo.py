#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Algorithms
"""

import sys
from abc import ABC, abstractproperty, abstractmethod
from artemis.core.properties import Properties
from artemis.logger import Logger
# TODO Create an interface class to AlgoBase to expose the run, finalize methods to framework
# Interface IAlgoBase class to expose the methods to the framework (apparently, I should not write a framework, see Fluent Python ... I am bored but probably getting paid)
# Concrete implementation of interface with AlgoBase
# Concrete base class provides the mixins or other ABCs
# Likely we want to provide the Job class instance to retrieve 
# job.histbook
# job.timers
# job.objectstore
# Inherited classes for user-defined methods MyAlgo


class AlgoBase():
    
    def __init__(self, name, **kwargs):
        self.__name = name
        self.properties = Properties()
        for key in kwargs:
            self.properties.add_property(key, kwargs[key])

    @property
    def name(self):
        return self.__name
    
    @property
    def hbook(self):
        return self._hbook

    @hbook.setter
    def hbook(self, hbook):
        self._hbook = hbook

    def lock(self):
        '''
        Lock all properties for algorithm
        '''
        self.properties.lock = True

    def initialize(self):
        '''
        Framework initialize
        '''
        pass

    def book(self):
        '''
        Book histograms
        '''
        pass 
    
    def execute(self, payload):
        '''
        Algo always accepts the output Node on a graph
        Data is accessed via the Parent.payload
        '''
        pass 
   
    def finalize(self):
        '''
        report timings, counters, etc..
        '''
        pass 

class TestAlgo(AlgoBase):
   
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

if __name__ == '__main__':

    testalgo = TestAlgo('test')
    testalgo.__run__('payload')



