#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""

"""
import unittest
import logging

from artemis.core.dag import Sequence, Chain, Menu
from artemis.algorithms.dummyalgo import DummyAlgo1
from artemis.algorithms.csvparseralgo import CsvParserAlgo
from artemis.algorithms.profileralgo import ProfilerAlgo
from artemis.core.singleton import Singleton
from artemis.core.properties import JobProperties
import artemis.io.protobuf.artemis_pb2 as artemis_pb2


logging.getLogger().setLevel(logging.INFO)


class ArtemisTestCase(unittest.TestCase):
        
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        self.menucfg = ''
        self.gencfg = ''
        self.prtcfg = ''
        testalgo = DummyAlgo1('dummy', myproperty='ptest', loglevel='INFO')
        csvalgo = CsvParserAlgo('csvparser', loglevel='INFO')
        profileralgo = ProfilerAlgo('profiler', loglevel='INFO')

        seq1 = Sequence(["initial"], (testalgo, testalgo), "seq1")
        seq2 = Sequence(["initial"], (testalgo, testalgo), "seq2")
        seq3 = Sequence(["seq1", "seq2"], (testalgo,), "seq3")
        seq4 = Sequence(["seq3"], (testalgo,), "seq4")

        dummyChain1 = Chain("dummy1")
        dummyChain1.add(seq1)
        dummyChain1.add(seq4)
        dummyChain1.add(seq3)
        dummyChain1.add(seq2)

        seq5 = Sequence(["initial"], (testalgo, testalgo), "seq5")
        seq6 = Sequence(["seq5"], (testalgo, testalgo), "seq6")
        seq7 = Sequence(["seq6"], (testalgo,), "seq7")

        dummyChain2 = Chain("dummy2")
        dummyChain2.add(seq5)
        dummyChain2.add(seq6)
        dummyChain2.add(seq7)

        csvChain = Chain("csvchain")
        seqX = Sequence(["initial"], (csvalgo,), "seqX")
        seqY = Sequence(["seqX"], (profileralgo,), "seqY")
        csvChain.add(seqX)
        csvChain.add(seqY)
        
        self.testmenu = Menu("test")
        self.testmenu.add(dummyChain1)
        self.testmenu.add(dummyChain2)
        self.testmenu.add(csvChain)
        self.testmenu.generate()

    def tearDown(self):
        Singleton.reset(JobProperties)
   
    def test_menu(self):
        msgmenu = self.testmenu.to_msg()
        try:
            with open('testmenu.dat', "wb") as f:
                f.write(msgmenu.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write message")
        except Exception:
            raise
        try:
            with open('testmenu.dat', 'rb') as f:
                msg = artemis_pb2.Menu()
                msg.ParseFromString(f.read())
        except Exception:
            raise



if __name__ == '__main__':
    unittest.main()
