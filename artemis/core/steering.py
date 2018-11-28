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
from collections import OrderedDict
from statistics import mean

from artemis.utils.utils import range_positive
from artemis.core.properties import JobProperties
from artemis.decorators import timethis
from artemis.core.physt_wrapper import Physt_Wrapper
from .algo import AlgoBase
from .tree import Tree, Node, Element


class Steering(AlgoBase):

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.__logger.info('%s: __init__ Steering' % self.name)
        self.__logger.debug('%s: __init__ Steering' % self.name)
        self.__logger.warning('%s: __init__ Steering' % self.name)
        self._chunk_cntr = 0
        # Execution graph
        self._menu = OrderedDict()
        self.jobops = JobProperties()
        self.__timers = dict()

    def initialize(self):
        self.__logger.info('Initialize Steering')
        self._seq_tree = Tree(self.jobops.meta.name)
        self.from_msg()

    def from_msg(self):
        '''
        Configure steering from a protobuf msg
        '''
        self.__logger.info('Loading menu from protobuf')
        msg = self.jobops.meta.config.menu
        self.__logger.info('Initializing Tree and Algos')

        # Initialize algorithms
        # Look up instance to add to execution graph
        algo_instances = {}
        for algo in msg.algos:
            try:
                algo_instances[algo.name] = \
                        AlgoBase.from_msg(self.__logger, algo)
            except Exception:
                self.__logger.error('Initializing from protobuf %s', algo.name)
                raise
            try:
                algo_instances[algo.name].initialize()
            except Exception:
                self.__logger.error("Cannot initialize algo %s" % algo.name)
                raise

        for node in msg.tree.nodes:
            self.__logger.debug("graph node %s" % (node.name))
            # Create the nodes for the tree
            if node.name == 'initial':
                self._seq_tree.root = Node(node.name, [])
                self._seq_tree.add_node(self._seq_tree.root)
            else:
                self._seq_tree.add_node(Node(node.name, node.parents))

            # Initialize the algorithms
            # Create the execution graph
            algos = []
            for algo in node.algos:
                self.__logger.debug("%s in graph node %s" % (algo, node.name))
                # TODO
                # Initial node has placeholder algo iorequest
                # iorequest algo is just a string name
                # no algo message, in json dict this is stored as an empt dict
                if node.name == 'initial':
                    algos.append('iorequest')
                    continue
                algos.append(algo_instances[algo])
            self._menu[node.name] = tuple(algos)

        self._seq_tree.update_parents()
        self._seq_tree.update_leaves()

        self.__logger.info('Tree nodes are as follows: %s' %
                           str(self._seq_tree.nodes))
        self.__logger.info('%s: Initialized Steering' % self.name)

    def lock(self):
        '''
        Overides the base class lock
        controls locking of all algorithm properties
        '''
        self.__logger.info("Lock Steering properties")
        self.properties.lock = True
        for key in self._menu:
            algos = self._menu[key]
            for algo in algos:
                if isinstance(algo, str):
                    continue
                algo.lock()

    def book(self):
        self.__logger.info("Book")
        self.hbook = Physt_Wrapper()
        # self.hbook.book(self.name, 'counts', range(100))
        # self.hbook.book(self.name, 'payload', range(100))
        # self.hbook.book(self.name, 'nblocks', range(100))
        # self.hbook.book(self.name, 'block_size', range(100))
        for key in self._menu:
            for algo in self._menu[key]:
                if isinstance(algo, str):
                    self.__logger.debug('Not an algo: %s' % algo)
                else:
                    self.__timers[algo.name] = list()
                    bins = [x for x in range_positive(0., 100., 2.)]
                    try:
                        self.hbook.book(self.name, 'time.'+algo.name,
                                        bins, 'ms')
                    except Exception:
                        self.__logger.error('Cannot book steering')
                        raise
                    try:
                        algo.book()
                    except Exception:
                        self.__logger.error('Cannot book %s' % algo.name)

    def _element_name(self, key):
        return self._seq_tree.name + \
                '_' + self._seq_tree.nodes[key].key + \
                '_' + str(self._chunk_cntr)

    def execute(self, payload):
        '''
        Prepares payload for algorithms
        Steers algorithm execution
        '''
        self.__logger.info('Execute %s' % self.name)

        for key in self._menu:
            algos = self._menu[key]
            self.__logger.debug('Menu input element: %s' % key)
            # TODO -- Continue work regarding gettting parent data, etc.
            self._seq_tree.nodes[key].payload.\
                append(Element(self._element_name(key)))
            self.__logger.debug('Element: %s' % self._element_name)
            if key == 'initial':
                self._seq_tree.nodes[key].payload[-1].add_data(payload)
            else:
                for parent in self._seq_tree.nodes[key].parents:
                    # When retrieving input data, we are duplicating data
                    # adding the input data as part of the new element
                    # with that element key
                    self._seq_tree.nodes[key].payload[-1].\
                        add_data(self._seq_tree.nodes[parent].payload[-1].
                                 get_data())

            for algo in algos:
                # TODO -- ensure the algos are actually type <class AlgoBase>
                if isinstance(algo, str):
                    self.__logger.debug('Not an algo: %s' % algo)
                else:
                    self.__logger.debug('Type: %s' % type(algo))
                    # Timing decorator / wrapper
                    _algexe = timethis(algo.execute)
                    try:
                        time_ = _algexe(self._seq_tree.nodes[key].
                                        payload[-1])[-1]
                        self.__timers[algo.name].append(time_)
                        self.hbook.fill(self.name, 'time.'+algo.name, time_)
                    except Exception:
                        raise

        self._chunk_cntr += 1

    def finalize(self):
        self.__logger.info("Completed steering")
        summary = self.jobops.meta.summary
        for key in self._menu:
            for algo in self._menu[key]:
                if isinstance(algo, str):
                    self.__logger.debug('Not an algo: %s' % algo)
                else:
                    algo.finalize()
        for key in self.__timers:
            _name = '.'
            _name = _name.join([self.name, 'time', key])
            mu = self.hbook.get_histogram(self.name, 'time.'+key).mean()
            std = self.hbook.get_histogram(self.name, 'time.'+key).std()
            self.__logger.info("%s timing: %2.4f" %
                               (key, mean(self.__timers[key])))
            self.__logger.info("%s timing: %2.4f" % (key, mu))
            msgtime = summary.timers.add()
            msgtime.name = _name
            msgtime.time = mu
            msgtime.std = std
