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
Efficient filtering algorithm to slim columns from input record batches and materialize new record batches.
"""

from artemis.core.algo import AlgoBase
from artemis.decorators import timethis
from artemis.utils.utils import range_positive
# Keep only specified columns from the record batches.
# Use invert option to remove only specified columns.


class FilterAlgo(AlgoBase):
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.__logger.info('%s: __init__ FilterAlgo' % self.name)

    def initialize(self):
        self.__logger.info('%s: Initialized FilterAlgo' % self.name)

    def book(self):
        self.__logger.info("Book")
        bins = [x for x in range_positive(0., 100., 2.)]
        self.gate.hbook.book(self.name, 'time.filtercol',
                             bins, 'ms', timer=True)

    def rebook(self):
        pass

    @timethis
    def filter_columns(self, record_batch):
        return self.get_tool('filtercoltool').execute(record_batch)

    def execute(self, element):
        fbatch, time_ = self.filter_columns(element.get_data())
        self.gate.hbook.fill(self.name, 'time.filtercol', time_)
        element.add_data(fbatch)

    def finalize(self):
        self.__logger.info("Completed FilterAlgo")
