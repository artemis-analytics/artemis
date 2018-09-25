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
    
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.__logger.info('%s: __init__ Steering' % self.name)           
        self.__logger.debug('%s: __init__ Steering' % self.name)            
        self.__logger.warning('%s: __init__ Steering' % self.name)            

    def initialize(self, job):
        self.hbook = job.hbook
        self._menu = job.menu
        for key in self._menu:
            algos = self._menu[key]
            for algo in algos:
                if isinstance(algo, str):
                    self.__logger.info('Algorithm name: %s', algo)
                else:
                    algo.hbook = job.hbook

        self.__logger.info('%s: Initialized Steering' % self.name)            
    
    def book(self):
        self.hbook[self.name + "_h1"] = "h1"

    def execute(self, payload):
        '''
        Prepares payload for algorithms
        Steers algorithm execution
        '''
        self.__logger.info('Execute %s' % self.name)
        
        for key in self._menu:
            algos = self._menu[key]
            self.__logger.debug('Menu input element: %s' % key)
            for algo in algos:
                # TODO -- ensure the algos are actually type <class AlgoBase>
                if isinstance(algo, str):
                    self.__logger.debug('Not an algo: %s' % algo)
                else:
                    self.__logger.debug('Type: %s' % type(algo))
                    algo.execute(payload)

    
