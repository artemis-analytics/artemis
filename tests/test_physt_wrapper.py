#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented 
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.


import numpy
import unittest

from artemis.core.singleton import Singleton
from artemis.core.physt_wrapper import Physt_Wrapper


class HistogramCase(unittest.TestCase):

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        self.data = numpy.random.normal(-5, 5, 10000)

    def tearDown(self):
        Singleton.reset(Physt_Wrapper)

    def test_histogram(self):
        self.data = numpy.random.normal(-5, 5, 10000)
        print('Starting histogram test')
        # Creating book.
        print('Create book')
        my_book = Physt_Wrapper()
        # Booking multiple histograms.
        print('Booking test0')
        my_book.book('physt', 'test0', range(-5, 5))
        print('Booking test1')
        my_book.book('physt', 'test1', range(-5, 5))
        print('Booking test2')
        my_book.book('physt', 'test2', range(-5, 5))
        print('Booking test3')
        my_book.book('physt', 'test3', range(-5, 5))
  
        # Filling multiple histograms.
        my_book.fill('physt', 'test0', self.data)
        for val in self.data:
            # Filling multiple histograms.
            my_book.fill('physt', 'test1', val)
            # Filling multiple histograms.
            my_book.fill('physt', 'test2', val)
            # Filling multiple histograms.
            my_book.fill('physt', 'test3', val)
        # Converting to pandas and print.
        print(my_book.to_pandas('physt', 'test0'))
        print(my_book.to_json('physt', 'test0'))
        print(my_book.to_pandas('physt', 'test1'))
        print(my_book.to_json('physt', 'test1'))
        print(my_book.to_pandas('physt', 'test2'))
        print(my_book.to_json('physt', 'test2'))
        print(my_book.to_pandas('physt', 'test3'))
        print(my_book.to_json('physt', 'test3'))
        print(my_book.get_histogram('physt', 'test0').mean())
        print(my_book.get_histogram('physt', 'test1').mean())
        print(my_book.get_histogram('physt', 'test2').mean())
        print(my_book.get_histogram('physt', 'test3').mean())
        print('Test end')


if __name__ == '__main__':
    unittest.main()
