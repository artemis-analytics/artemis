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
import os

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
        self.folder = 'cnvtcsv'
        #self.ncol = [10, 50, 100]
        #self.nrow = [10000, 100000, 500000, 1000000]
        #self.blck = [1*(1024**2), 10*(1024**2), 100*(1024**2), 500*(1024**2) ]
        self.ncol = [10, 50, 100]
        self.nrow = [10000, 50000]
        self.blck = [1*(1024**2), 10*(1024**2)]
        self.nbatches = 1

        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

        try:
            os.makedirs(self.folder)
        except Exception:
            raise

        csvalgo = CsvParserAlgo('csvparser')
        csvChain = Chain("csvchain")
        seqX = Sequence(["initial"], (csvalgo,), "seqX")
        csvChain.add(seqX)

        testmenu = Menu("test")
        testmenu.add(csvChain)
        testmenu.generate()
        try:
            testmenu.to_json(self.folder + '/' + self.menucfg)
        except Exception:
            raise

        for col in self.ncol:
            for row in self.nrow:
                for blck in self.blck:
                    generator = GenCsvLikeArrow('generator',
                                                nbatches=self.nbatches,
                                                num_cols=col,
                                                num_rows=row)
                    try:
                        with open((self.folder + '/' + str(col) + '_' + str(row) + '_' + str(blck) + '_' + self.gencfg), 'x') as ofile:
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
                for blck in self.blck:
                    prefix_name = str(col) + '_' + str(row) + '_' + str(blck) + '_'
                    print("Testing the Artemis Prototype")
                    bow = Artemis((self.folder + '/' + prefix_name + "cnvtcsv"), 
                                  menu=self.folder + '/' + self.menucfg, 
                                  generator=(self.folder + '/' + prefix_name + self.gencfg),
                                  blocksize=blck,
                                  skip_header=True,
                                  loglevel='INFO')
                    bow.control()
                    
                    # get the JobProperties with results
                    jp = JobProperties()
                    print(jp.data['results'])

if __name__ == '__main__':
    unittest.main()

