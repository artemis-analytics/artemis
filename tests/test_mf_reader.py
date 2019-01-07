#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Dominic Parent <dominic.parent@canada.ca>
#
# Distributed under terms of the  license.


import unittest
from collections import OrderedDict, namedtuple

class Test_MF_Reader(unittest.TestCase):

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        pass

    def test_mf_reader(self):
        rsize = 20
        nrecords = 3
        csize = rsize * nrecords
        idata = "012345678iabcd01234m012345678iabcd01234m012345678iabcd01234m012345678iabcd01234m012345678iabcd01234m012345678iabcd01234m012345678iabcd01234m012345678iabcd01234m012345678iabcd01234m012345678iabcd01234m012345678iabcd01234m012345678iabcd01234m012345678iabcd01234m012345678iabcd01234m012345678iabcd01234m"
        isize = len(idata)
        schema = [10,4,6]
        odata = []

        for field in schema:
            odata.append([])

        counter = 0

        chunk = idata[counter:csize]
        ncounter = 0
        while ncounter < nrecords:
            odata[ncounter].append(chunk[counter:schema[ncounter]])
            ncounter = ncounter + 1
            counter = counter + schema[ncounter]

            print(odata)
