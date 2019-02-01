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
        intconf1 = {'utype':'uint', 'length':6}
        strconf0 = {'utype':'str', 'length':4}
        self.schema = [intconf0, strconf0, intconf1]
        self.block = "012345678aabcd012345012345678babcd012345"\
                 + "012345678cabc 012345012345678dabcd012345"\
                 + "012345678eabcd012345012345678fabcd012345"\
                 + "012345678aabc 012345012345678babcd012345"\
                 + "012345678cabcd012345012345678dabcd012345"\
                 + "012345678eabc 012345012345678fabcd012345"\
                 + "012345678aabcd012345012345678babcd012345"\
                 + "012345678cabc 012345"
        mfreader = MfTool(self.schema)
        mfreader.execute(self.schema, self.block)
