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
        intconf0 = {'utype':'int', 'length':10}
        intconf1 = {'utype':'int', 'length':6}
        strconf0 = {'utype':'str', 'length':4}
        self.schema = [intconf0, strconf0, intconf1]
        self.block = "012345678aabcd01234m012345678babcd01234m"\
                 + "012345678cabc 01234m012345678dabcd01234m"\
                 + "012345678eabcd01234m012345678fabcd01234m"\
                 + "012345678aabc 01234m012345678babcd01234m"\
                 + "012345678cabcd01234m012345678dabcd01234m"\
                 + "012345678eabc 01234m012345678fabcd01234m"\
                 + "012345678aabcd01234m012345678babcd01234m"\
                 + "012345678cabc 01234n"
        mfreader = MfTool(self.schema)
        mfreader.execute(self.schema, self.block)
