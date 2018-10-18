#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""

"""
import unittest

from artemis.core.algo import AlgoBase
import logging
from artemis.core.singleton import Singleton
from artemis.core.datastore import ArrowSets
from artemis.generators.generators import GenCsvLike, GenCsvLikeArrow
from pprint import pformat
import sys 

logging.getLogger().setLevel(logging.DEBUG)

class AlgoTestCase(unittest.TestCase):
    class TestAlgo(AlgoBase):
       
        def __init__(self, name, **kwargs):
            super().__init__(name, **kwargs)
            self.__logger.debug(pformat(kwargs))
            self.__logger.debug(pformat(self.__dict__))
            self.__logger.debug(pformat(self.__class__.__dict__))
            self.__logger.info('%s: Initialized DummyAlgo1' % self.name)
            self.store = ArrowSets()
        
        def initialize(self):
            self.__logger.info(self.__logger)
            self.__logger.info(self._TestAlgo__logger)
            self.__logger.info('%s: property %s' % (self.name, self.properties.myproperty))

        def book(self):
            pass

        def execute(self, payload):
            if(logging.getLogger().isEnabledFor(logging.DEBUG) or
                    self.__logger.isEnabledFor(logging.DEBUG)):

                # Prevent excessive formating calls when not required
                # Note that we can indepdently change the logging level 
                # for algo loggers and root logger
                # Use string interpolation to prevent excessive format calls
                self.__logger.debug('%s: execute ' % self.name)
                # Check logging level if formatting requiered
                self.__logger.debug('{}: execute: payload {}'.format(self.name, sys.getsizeof(payload)))
            
            self.__logger.debug("Trying to debug")
            my_data = self.store.get_data(payload)
            my_data2 = []
            count = 0
            my_sum = 0
            avg1 = 0
            avg2 = 0

            print('my_data: ' + str(my_data))

            for value in my_data:
                my_sum += value
                count += 1
                value += 1
                my_data2.append(value)
            avg1 = my_sum/count
            count = 0
            my_sum = 0

            print('my_data, redux: ' + str(my_data2))

            for value in my_data2:
                my_sum += value
                count += 1
            avg2 = my_sum/count

            print('Average 1 equals: ' + str(avg1))
            print('Average 2 equals: ' + str(avg2))
            print('Average 2 minus average 1 equals: ' + str(avg2 - avg1))

            self.store.add_to_dict('test1', my_data2)
            self.store.add_to_dict('avg1', avg1)
            self.store.add_to_dict('avg2', avg2)

            print('Datastore')
            print(self.store.arrow_dict)




        def finalize(self):
            pass
    
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
    
    def tearDown(self):
        Singleton.reset(ArrowSets)
    
    def test_algo(self):
        self.testalgo = self.TestAlgo("testalgo", myproperty='ptest', loglevel='DEBUG')
        print("Name", self.testalgo.name)
        print(self.testalgo.__dict__)
        self.testalgo.initialize()
        print("Name", self.testalgo.name)
        print(self.testalgo.__dict__)
        print(self.testalgo.properties.myproperty)
        store = ArrowSets()
        store.add_to_dict('test0', [1, 2, 3])
        
        self.testalgo.execute('test0')

    def test_gen_flow(self):
        generator = GenCsvLike()
        generator.nchunks = 1
        ichunk = 0
        for chunk in generator.generate():
            print('Test chunk %s' % ichunk)
            ichunk += 1
   
    def chunker(self):
        nbatches = 1
        generator = GenCsvLikeArrow()
        for ibatch in range(nbatches):
            yield generator.make_random_csv()

    def test_chunker(self):
        for batch in self.chunker():
            print('Batch test')
            print(batch)

if __name__ == '__main__':
    unittest.main()
