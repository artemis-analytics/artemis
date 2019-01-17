#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Dominic Parent <dominic.parent@canada.ca>
#
# Distributed under terms of the  license.


import unittest
from collections import OrderedDict, namedtuple

import pyarrow as pa

from artemis.tools.mftool import MfTool
from artemis.core.tool import ToolBase

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
        arrowodata = []
        data_types = ['signedint', 'string', 'signedint']
        pos_chars = {'{':'0', 'a':'1', 'b':'2', 'c':'3', 'd':'4', 'e':'5', 'f':'6', 'g':'7', 'h':'8', 'i':'9'}
        neg_chars = {'j':'0', 'k':'1', 'l':'2', 'm':'3', 'n':'4', 'o':'5', 'p':'6', 'q':'7', 'r':'8', 's':'9'}

        for field in schema:
            odata.append([])

        icounter = 0
        ccounter = 0
        ncounter = 0
        fcounter = 0

        while icounter < isize:
            cdata = idata[icounter:(icounter + csize)] # Extract chunks.
            while ccounter < csize:
                rdata = cdata[ccounter: (ccounter + rsize)] # Extract records.
                while ncounter < nrecords:
                    record = rdata[fcounter:(fcounter + schema[ncounter])] # Extract fields.
                    if data_types[ncounter] == 'signedint':
                        if record[-1:] in pos_chars:
                            record = int(record.replace(record[-1:], pos_chars[record[-1:]]))
                        else:
                            record = record.replace(record[-1:], neg_chars[record[-1:]])
                            record = int('-' + record)
                        odata[ncounter].append(record)
                    elif data_types[ncounter] == 'string':
                        odata[ncounter].append(record)
                    fcounter = fcounter + schema[ncounter]
                    ncounter = ncounter + 1
                ncounter = 0
                fcounter = 0
                ccounter = ccounter + rsize
            icounter = icounter + csize
            ccounter = 0

        counter = 0
        for my_list in odata:
            if data_types[counter] == 'signedint':
                arrowodata.append(pa.array(my_list))
            else:
                arrowodata.append(pa.array(my_list))
            counter = counter + 1

        print('Output data lists.')
        print(odata)

        print('Output data arrow arrays.')
        print(arrowodata)

    def test_tool_mf_reader(self):
        MfTool.execute('testself', 'testblock')
