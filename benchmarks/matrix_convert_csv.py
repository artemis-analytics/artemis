#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
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
        #self.ncol = [10, 50, 100]
        #self.nrow = [10000, 100000, 500000, 1000000]
        self.ncol = [10, 50, 100]
        self.nrow = [10000, 50000]
        self.blck = [1*(1024**2), 10*(1024**2), 100*(1024**2), 500*(1024**2) ]

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

        for col in self.ncol:
            for row in self.nrow:
                generator = GenCsvLikeArrow('generator',
                                            nbatches=1,
                                            num_cols=col,
                                            num_rows=row)
                try:
                    with open((str(col) + '_' + str(row) + '_' + self.gencfg), 'x') as ofile:
                        json.dump(generator.to_dict(), 
                                  ofile, 
                                  indent=4)
                except Exception:
                    raise

    def tearDown(self):
        Singleton.reset(JobProperties)

    def test_control(self):
        for col in self.ncol:
            for row in self.nrow:
                prefix_name = str(col) + '_' + str(row) + '_'
                print("Testing the Artemis Prototype")
                bow = Artemis((prefix_name + "cnvtcsv"), 
                              menu=self.menucfg, 
                              generator=(prefix_name + self.gencfg),
                              blocksize=2**16,
                              skip_header=True,
                              loglevel='INFO')
                bow.control()
                
                # get the JobProperties with results
                jp = JobProperties()
                print(jp.data['results'])


if __name__ == '__main__':
    unittest.main()

