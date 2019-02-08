#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented 
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""
Initial benchmarking framework for simulated csv data
"""

import unittest
import logging
import json

from artemis.core.dag import Sequence, Chain, Menu
from artemis.algorithms.dummyalgo import DummyAlgo1
from artemis.algorithms.csvparseralgo import CsvParserAlgo
from artemis.artemis import Artemis
from artemis.core.singleton import Singleton
from artemis.core.properties import JobProperties
from artemis.generators.generators import GenCsvLikeArrow

logging.getLogger().setLevel(logging.INFO)


class ArtemisTestCase(unittest.TestCase):

    def setUp(self):
        self.menucfg = 'cnvtcsv_menu.json'
        self.gencfg = 'cnvtcsv_gen.json'

        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        csvalgo = CsvParserAlgo('csvparser')
        csvChain = Chain("csvchain")
        seqX = Sequence(["initial"], (csvalgo,), "seqX")
        csvChain.add(seqX)

        testmenu = Menu("test")
        testmenu.add(csvChain)
        testmenu.generate()
        try:
            testmenu.to_json(self.menucfg)
        except Exception:
            raise

        generator = GenCsvLikeArrow('generator',
                                    nbatches=1,
                                    num_cols=20,
                                    num_rows=10000)
        try:
            with open(self.gencfg, 'x') as ofile:
                json.dump(generator.to_dict(), 
                          ofile, 
                          indent=4)
        except Exception:
            raise

    def tearDown(self):
        Singleton.reset(JobProperties)

    def test_control(self):
        print("Testing the Artemis Prototype")
        bow = Artemis("cnvtcsv", 
                      menu=self.menucfg, 
                      generator=self.gencfg,
                      blocksize=2**16,
                      skip_header=True,
                      loglevel='INFO')
        bow.control()
        
        # get the JobProperties with results
        jp = JobProperties()
        print(jp.data['results'])


if __name__ == '__main__':
    unittest.main()

