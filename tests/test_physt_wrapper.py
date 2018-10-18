#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Dominic Parent <dominic.parent@canada.ca>
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
        self.data = numpy.random.normal(-5, 5, 1000000)

    def tearDown(self):
        Singleton.reset(Physt_Wrapper)

    def test_histogram(self):
        self.data = numpy.random.normal(-5, 5, 1000000)
        print('Starting histogram test')
        # Creating book.
        print('Create book')
        my_book = Physt_Wrapper()
        # Booking multiple histograms.
        print('Booking test0')
        my_book.book('test0')
        print('Booking test1')
        my_book.book('test1')
        print('Booking test2')
        my_book.book('test2')
        print('Booking test3')
        my_book.book('test3')
        # Filling multiple histograms.
        print('Fill test0')
        my_book.fill('test0', self.data, 10)
        print('Fill test1')
        my_book.fill('test1', self.data, 20)
        print('Fill test2')
        my_book.fill('test2', self.data, 10)
        print('Fill test3')
        my_book.fill('test3', self.data, 10)
        # Converting to pandas and print.
        print(my_book.to_pandas('test0'))
        print(my_book.to_json('test0'))
        print(my_book.to_pandas('test1'))
        print(my_book.to_json('test1'))
        print(my_book.to_pandas('test2'))
        print(my_book.to_json('test2'))
        print(my_book.to_pandas('test3'))
        print(my_book.to_json('test3'))
        print('Test end')
