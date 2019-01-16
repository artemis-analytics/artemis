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
        idata = "012345678aabcd01234m012345678babcd01234m012345678cabcd01234m012345678dabcd01234m012345678eabcd01234m012345678fabcd01234m012345678aabcd01234m012345678babcd01234m012345678cabcd01234m012345678dabcd01234m012345678eabcd01234m012345678fabcd01234m012345678aabcd01234m012345678babcd01234m012345678cabcd01234m"
        isize = len(idata)
        schema = [10,4,6]
        odata = []

        for field in schema:
            odata.append([])

        icounter = 0
        ccounter = 0
        ncounter = 0
        fcounter = 0

        while icounter < isize:
            cdata = idata[icounter:(icounter + csize)]
            print('Data')
            print(cdata)
            while ccounter < csize:
                rdata = cdata[ccounter: (ccounter + rsize)]
                print('Record')
                print(rdata)
                while ncounter < nrecords:
                    odata[ncounter].append(rdata[fcounter:(fcounter + schema[ncounter])])
                    print('Field')
                    print(odata)
                    fcounter = fcounter + schema[ncounter]
                    ncounter = ncounter + 1
                ncounter = 0
                fcounter = 0
                ccounter = ccounter + rsize
            icounter = icounter + csize
            ccounter = 0
