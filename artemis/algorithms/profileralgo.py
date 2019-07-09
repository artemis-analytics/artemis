#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""
Placeholder algo to profile a RecordBatch
"""

from artemis.core.algo import AlgoBase
from artemis.core.properties import JobProperties
from artemis.core.physt_wrapper import Physt_Wrapper


class ProfilerAlgo(AlgoBase):

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.__logger.info('%s: __init__ ProfilerAlgo' % self.name)
        self.reader = None
        self.jobops = None

    def initialize(self):
        self.jobops = JobProperties()
        self.__logger.info('%s: Initialized ProfilerAlgo' % self.name)

    def book(self):
        self.__logger.info("Book")
        self.hbook = Physt_Wrapper()

    def execute(self, element):

        raw_ = element.get_data()
        self.__logger.debug('Num cols: %s Num rows: %s',
                            raw_.num_columns, raw_.num_rows)

        # Redundant, it is the same object as the input!
        element.add_data(raw_)

    def finalize(self):
        self.__logger.info("Completed Profiling")
