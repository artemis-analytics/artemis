#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Dominic Parent <dominic.parent@canada.ca>
#
# Distributed under terms of the  license.

import unittest

from artemis.generators.generators import GenMF

class Test_MF_Gen(unittest.TestCase):

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        pass

    def test_dev(self):
        intconf0 = {'utype':'int', 'length':10, 'min_val':0, 'max_val':10}
        intconf1 = {'utype':'int', 'length':10, 'min_val':0, 'max_val':10}
        intconf2 = {'utype':'int', 'length':10, 'min_val':0, 'max_val':10}
        intconf3 = {'utype':'int', 'length':10, 'min_val':0, 'max_val':10}
        intconf4 = {'utype':'int', 'length':10, 'min_val':0, 'max_val':10}
        intconf5 = {'utype':'int', 'length':10, 'min_val':0, 'max_val':10}
        intconf6 = {'utype':'int', 'length':10, 'min_val':0, 'max_val':10}
        intconf7 = {'utype':'int', 'length':10, 'min_val':0, 'max_val':10}
        intconf8 = {'utype':'int', 'length':10, 'min_val':0, 'max_val':10}
        intconf9 = {'utype':'int', 'length':10, 'min_val':0, 'max_val':10}
        strconf0 = {'utype':'str', 'length':10}
        strconf1 = {'utype':'str', 'length':10}
        strconf2 = {'utype':'str', 'length':10}
        strconf3 = {'utype':'str', 'length':10}
        strconf4 = {'utype':'str', 'length':10}
        strconf5 = {'utype':'str', 'length':10}
        strconf6 = {'utype':'str', 'length':10}
        strconf7 = {'utype':'str', 'length':10}
        strconf8 = {'utype':'str', 'length':10}
        strconf9 = {'utype':'str', 'length':10}
        test_ds = [intconf0, intconf1, strconf0, intconf2, strconf1, 
                   strconf2, intconf3, intconf4, intconf5, strconf3, 
                   intconf6, strconf4, strconf5, strconf6, intconf7,
                   strconf7, strconf8, intconf8, intconf9, strconf9]
        size = 10
        GenMF.gen_column('test', intconf0, size)
        GenMF.gen_column('test', strconf0, size)
        GenMF.gen_chunk('test', test_ds, size)
