#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Steering 
"""

from .algo import AlgoBase


class Steering(AlgoBase):
    
    def __init__(self, name):
        super().__init__(name)
   
    def initialize(self, job):
        self.hbook = job.hbook
        self._menu = job.menu
        for key in self._menu:
            algos = self._menu[key]
            for algo in algos:
                if isinstance(algo, str):
                    print(algo)
                else:
                    algo.hbook = job.hbook
    
    def book(self):
        self.hbook[self.name + "_h1"] = "h1"

    def execute(self, payload):
        '''
        Prepares payload for algorithms
        Steers algorithm execution
        '''
        self.info('{}: Execute'.format(self.name))
        
        for key in self._menu:
            algos = self._menu[key]
            self.debug('{}: {}'.format(self.name, key))
            for algo in algos:
                if isinstance(algo, str):
                    print(algo)
                else:
                    algo.execute(payload)

    
