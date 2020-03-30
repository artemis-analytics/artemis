#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Menu for generating and processing Legacy data
"""
from artemis.configurables.configurable import MenuBuilder
from artemis.algorithms.legacyalgo import LegacyDataAlgo
from artemis.meta.Directed_Graph import Directed_Graph, Node


class LegacyGenMenu(MenuBuilder):
    def __init__(self, name="test"):
        super().__init__(name)

    def _algo_builder(self):
        """
        define all algorithms required
        """
        self._algos["legacyalgo"] = LegacyDataAlgo("legacyparser", loglevel="INFO")

    def _seq_builder(self):
        self._seqs["seqX"] = Node(["initial"], ("legacyparser",), "seqX")

    def _chain_builder(self):
        self._chains["legacyChain"] = Directed_Graph("legacychain")
        self._chains["legacyChain"].add(self._seqs["seqX"])
