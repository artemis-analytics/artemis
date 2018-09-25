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
from .tree import Tree, Node, Element

class Steering(AlgoBase):
    
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.__logger.info('%s: __init__ Steering' % self.name)           
        self.__logger.debug('%s: __init__ Steering' % self.name)            
        self.__logger.warning('%s: __init__ Steering' % self.name)            

    def initialize(self, job):
        self.hbook = job.hbook
        self._menu = job.menu
        self._seq_tree = Tree(job.jobname)
        self._chunk_cntr = 0
        for key in self._menu:
            algos = self._menu[key].algos
            for algo in algos:
                if isinstance(algo, str):
                    self.__logger.info('Algorithm name: %s', algo)
                else:
                    algo.hbook = job.hbook
            if key == 'initial':
                self._seq_tree.root = Node(key, self._menu[key].parents)
                self._seq_tree.add_node(self._seq_tree.root)
            else:
                self._seq_tree.add_node(Node(key, self._menu[key].parents))
        self._seq_tree.update_parents()
        self._seq_tree.update_leaves()

        self.__logger.info('Tree nodes are as follows: %s' % str(self._seq_tree.nodes))
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
            algos = self._menu[key].algos
            self.__logger.debug('Menu input element: %s' % key)
            for algo in algos:
                # TODO -- ensure the algos are actually type <class AlgoBase>
                if isinstance(algo, str):
                    self.__logger.debug('Not an algo: %s' % algo)
                else:
                    self.__logger.debug('Type: %s' % type(algo))
                    algo.execute(payload)
            self._seq_tree.nodes[key].payload.append(Element(self._seq_tree.name + '_' + self._seq_tree.nodes[key].key + '_' + str(self._chunk_cntr)))
            print('Print the name of the element: ' + self._seq_tree.name + '_' + self._seq_tree.nodes[key].key + '_' + str(self._chunk_cntr))
        self._chunk_cntr += 1
