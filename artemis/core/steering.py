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
    
    def __init__(self, name):
        super().__init__(name)
   
    def initialize(self, job):
        self.hbook = job.hbook
        self._menu = job.menu
        self._seq_tree = Tree(job.jobname)
        self._chunk_cntr = 0
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
        self._seq_tree.update_parents()
        self._seq_tree.update_leaves()

        print('Tree nodes are as follows:' + str(self._seq_tree.nodes))
    
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
            self._seq_tree.nodes[key].payload.append(Element(self._seq_tree.name + '_' + self._seq_tree.nodes[key].key + '_' + str(self._chunk_cntr)))
            print('Print the name of the element: ' + self._seq_tree.name + '_' + self._seq_tree.nodes[key].key + '_' + str(self._chunk_cntr))
        self._chunk_cntr += 1
