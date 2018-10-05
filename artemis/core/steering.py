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
import importlib
from pprint import pformat

from artemis.core.properties import JobProperties

from .algo import AlgoBase
from .tree import Tree, Node, Element


class Steering(AlgoBase):

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.__logger.info('%s: __init__ Steering' % self.name)
        self.__logger.debug('%s: __init__ Steering' % self.name)
        self.__logger.warning('%s: __init__ Steering' % self.name)

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
                    module = importlib.import_module(
                            graphcfg['algos'][algo]['module']
                            )
                except ImportError:
                    self.__logger.error('Unable to load module ',
                                        graphcfg['algos'][algo]['module'])
                    raise
                except Exception as e:
                    self.__logger.error("Unknow error loading module: %s" % e)
                    raise
                try:
                    class_ = getattr(module, graphcfg['algos'][algo]['class'])
                except AttributeError:
                    self.__logger.error("%s: missing attribute %s" %
                                        (algo, 'class'))
                    raise
                except Exception as e:
                    self.__logger.error("Reason: %s" % e)
                    raise

                self.__logger.debug("from_dict: {}".format(algo))
                self.__logger.debug(pformat(
                                    graphcfg['algos'][algo]['properties']))

                # Update the logging level of
                # algorithms if loglevel not set
                # Ensures user-defined algos get the artemis level logging
                if 'loglevel' not in graphcfg['algos'][algo]['properties']:
                    graphcfg['algos'][algo]['properties']['loglevel'] = \
                            self.__logger.getEffectiveLevel()

                try:
                    instance = class_(algo,
                                      **graphcfg['algos'][algo]['properties']
                                      )
                except AttributeError:
                    self.__logger.error("%s: missing attribute %s" %
                                        (algo, 'properties'))
                    raise
                except Exception as e:
                    self.__logger.error("%s: Cannot initialize %s" % e)
                    raise

                algos.append(instance)
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
        pass
        # self.hbook[self.name + "_h1"] = "h1"

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
            for algo in algos:
                # TODO -- ensure the algos are actually type <class AlgoBase>
                if isinstance(algo, str):
                    self.__logger.debug('Not an algo: %s' % algo)
                else:
                    self.__logger.debug('Type: %s' % type(algo))
                    algo.execute(payload)
            self._seq_tree.nodes[key].payload.append(
                    Element(self._element_name(key)))
            self.__logger.debug('Element: %s' % self._element_name)
        self._chunk_cntr += 1
