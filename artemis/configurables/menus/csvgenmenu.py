#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""

"""
from artemis.configurables.configurable import MenuBuilder
from artemis.core.dag import Sequence, Chain
from artemis.algorithms.dummyalgo import DummyAlgo1
from artemis.algorithms.csvparseralgo import CsvParserAlgo
from artemis.algorithms.profileralgo import ProfilerAlgo


class CsvGenMenu(MenuBuilder):

    def __init__(self, name='test'):
        super().__init__(name)

    def _algo_builder(self):
        '''
        define all algorithms required
        '''
        self._algos['testalgo'] = DummyAlgo1('dummy',
                                             myproperty='ptest',
                                             loglevel='INFO')
        self._algos['csvalgo'] = CsvParserAlgo('csvparser', loglevel='INFO')
        self._algos['profileralgo'] = ProfilerAlgo('profiler', loglevel='INFO')

    def _seq_builder(self):
        self._seqs['seq1'] = Sequence(["initial"],
                                      (self._algos['testalgo'],
                                       self._algos['testalgo']),
                                      "seq1")
        self._seqs['seq2'] = Sequence(["initial"],
                                      (self._algos['testalgo'],
                                       self._algos['testalgo']),
                                      "seq2")
        self._seqs['seq3'] = Sequence(["seq1", "seq2"],
                                      (self._algos['testalgo'],),
                                      "seq3")
        self._seqs['seq4'] = Sequence(["seq3"],
                                      (self._algos['testalgo'],),
                                      "seq4")

        self._seqs['seq5'] = Sequence(["initial"],
                                      (self._algos['testalgo'],
                                       self._algos['testalgo']),
                                      "seq5")
        self._seqs['seq6'] = Sequence(["seq5"],
                                      (self._algos['testalgo'],
                                       self._algos['testalgo']),
                                      "seq6")
        self._seqs['seq7'] = Sequence(["seq6"],
                                      (self._algos['testalgo'],),
                                      "seq7")

        self._seqs['seqX'] = Sequence(["initial"],
                                      (self._algos['csvalgo'],),
                                      "seqX")
        self._seqs['seqY'] = Sequence(["seqX"],
                                      (self._algos['profileralgo'],),
                                      "seqY")

    def _chain_builder(self):
        self._chains['dummyChain1'] = Chain("dummy1")
        self._chains['dummyChain1'].add(self._seqs['seq1'])
        self._chains['dummyChain1'].add(self._seqs['seq4'])
        self._chains['dummyChain1'].add(self._seqs['seq3'])
        self._chains['dummyChain1'].add(self._seqs['seq2'])

        self._chains['dummyChain2'] = Chain("dummy2")
        self._chains['dummyChain2'].add(self._seqs['seq5'])
        self._chains['dummyChain2'].add(self._seqs['seq6'])
        self._chains['dummyChain2'].add(self._seqs['seq7'])

        self._chains['csvchain'] = Chain("csvchain")
        self._chains['csvchain'].add(self._seqs['seqX'])
        self._chains['csvchain'].add(self._seqs['seqY'])
