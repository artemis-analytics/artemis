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
from .tree import Tree, Node

class Steering(AlgoBase):
    
    def __init__(self, name):
        super().__init__(name)
   
    def initialize(self, job):
        self.hbook = job.hbook
        self._menu = job.menu
        self._seq_tree = Tree()
        for key in self._menu:
            algos = self._menu[key].algos
            for algo in algos:
                if isinstance(algo, str):
                    print(algo)
                else:
                    algo.hbook = job.hbook
            if key == 'initial':
                self._seq_tree.root = Node(key, self._menu[key].parents)
                self._seq_tree.add_node(self._seq_tree.root)
            else:
                self._seq_tree.add_node(Node(key, self._menu[key].parents))
                for parent in self._seq_tree.nodes[key].parents:
                    self._seq_tree.nodes[parent].children.append(key)
        print(self._seq_tree.nodes)
    
    def book(self):
        self.hbook[self.name + "_h1"] = "h1"

    def execute(self, payload):
        '''
        Prepares payload for algorithms
        Steers algorithm execution
        '''
        for key in self._menu:
            algos = self._menu[key].algos
            print(key)
            for algo in algos:
                if isinstance(algo, str):
                    print(algo)
                else:
                    algo.execute(payload)

    
