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
from pprint import pformat
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
        self.__timers = dict()

    def initialize(self):
        self.__logger.info('Initialize Steering')
        # self.hbook = job.hbook
        self.jobops = JobProperties()
        try:
            self._validate_menu()
        except KeyError:
            self.__logger.error('Cannot validate menu')
            raise
        self.__logger.info('Menu validated')

        graphcfg = self.jobops.data['menu']
        self._chunk_cntr = 0
        # Execution graph
        self._menu = OrderedDict()
        self.__logger.debug(pformat(graphcfg))

        self._seq_tree = Tree(self.jobops.data['job']['jobname'])

        self.__logger.debug(pformat(graphcfg))
        self.__logger.info('Initializing Tree and Algos')
        for key in graphcfg['graph']:
            self.__logger.debug("graph node %s" % (key))
            # Create the nodes for the tree
            if key == 'initial':
                self._seq_tree.root = Node(key, [])
                self._seq_tree.add_node(self._seq_tree.root)
            else:
                if key in graphcfg['tree']:
                    self._seq_tree.add_node(Node(key, graphcfg['tree'][key]))
                else:
                    self.__logger.error("%s node not found in tree" % key)

            # Initialize the algorithms
            # Create the execution graph
            algos = []
            for algo in graphcfg['graph'][key]:
                self.__logger.debug("%s in graph node %s" % (algo, key))
                if graphcfg['algos'][algo] is None:
                    algos.append(algo)
                    continue
                elif len(graphcfg['algos'][algo].items()) == 0:
                    algos.append(algo)
                    continue
                try:
                    instance = AlgoBase.load(self.__logger,
                                             **graphcfg['algos'][algo])
                except Exception:
                    self.__logger.error('Error loading %s', algo)
                    raise

                algos.append(instance)
                try:
                    instance.initialize()
                except Exception:
                    self.__logger.error("Cannot initialize algo %s" % algo)
                    raise
                self.__logger.debug("from_dict: instance {}".format(instance))
            self._menu[key] = tuple(algos)

        self._seq_tree.update_parents()
        self._seq_tree.update_leaves()

        self.__logger.info('Tree nodes are as follows: %s' %
                           str(self._seq_tree.nodes))
        self.__logger.info('%s: Initialized Steering' % self.name)

    def _validate_menu(self):
        '''
        Internal method to validate the expected
        menu layout
        essentially validating the data model
        better ways to do this ...
        Move to staticmethod in dag.py
        '''
        self.__logger.debug("Validate menu")
        self.__logger.debug(pformat(self.jobops.data['menu']))
        self.__logger.debug(self.jobops.data.keys())
        if 'menu' not in self.jobops.data.keys():
            self.__logger.error("KeyError %s" % 'menu')
            raise KeyError('menu')

        keys = ['graph', 'tree', 'algos']
        for key in keys:
            if key not in self.jobops.data['menu'].keys():
                raise KeyError(key)

    def lock(self):
        '''
        Overides the base class lock
        controls locking of all algorithm properties
        '''
        self.properties.lock = True
        for key in self._menu:
            algos = self._menu[key]
            for algo in algos:
                if isinstance(algo, str):
                    continue
                algo.lock()

    def to_dict(self):
        # TODO
        # Reuse existing base class code
        # extend the dictionary somehow
        print(super().to_dict())
        _dict = OrderedDict()
        _dict['name'] = self.name
        _dict['class'] = self.__class__.__name__
        _dict['module'] = self.__module__
        _dict['properties'] = self.properties.to_dict()
        _dict['graph'] = OrderedDict()
        for key in self._menu:
            _dict['graph'][key] = OrderedDict()
            algos = self._menu[key]
            for algo in algos:
                if isinstance(algo, str):
                    _dict['graph'][key][algo] = OrderedDict()
                else:
                    _dict['graph'][key][algo.name] = algo.to_dict()
        return _dict

    def book(self):
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
        self.__logger.info("Finalize Algos")
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
            self.__logger.info("%s timing: %2.4f" %
                               (key, mean(self.__timers[key])))
            self.__logger.info("%s timing: %2.4f" % (key, mu))
            try:
                self.jobops.data['results'][_name] = mu
            except KeyError:
                self.__logger.warning('Error in JobProperties')
                # Do not raise, just issue warning
