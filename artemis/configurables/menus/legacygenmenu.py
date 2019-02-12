#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Menu for generating and processing Legacy data
"""

from artemis.configurables.configurable import MenuBuilder
from artemis.core.dag import Sequence, Chain
from artemis.algorithms.legacyalgo import LegacyDataAlgo


class LegacyGenMenu(MenuBuilder):

    def __init__(self, name='test'):
        super().__init__(name)

    def _algo_builder(self):
        '''
        define all algorithms required
        '''
        self._algos['legacyalgo'] = LegacyDataAlgo('legacyparser',
                                                   loglevel='INFO')

    def _seq_builder(self):
        self._seqs['seqX'] = Sequence(["initial"],
                                      (self._algos['legacyalgo'],),
                                      "seqX")

    def _chain_builder(self):
        self._chains['legacyChain'] = Chain("legacychain")
        self._chains['legacyChain'].add(self._seqs['seqX'])
