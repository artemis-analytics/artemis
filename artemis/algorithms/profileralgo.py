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
        _finfo = self.jobops.meta.data[-1]
        schema = [x.name for x in _finfo.schema.columns]
        self.__logger.debug('Expected header %s' % schema)
        num_cols = raw_.num_columns
        num_rows = raw_.num_rows

        if len(schema) != num_cols:
            self.__logger.error("Expected schema length not found in table")
        else:
            self.__logger.debug("Records %s", num_rows)

        # Redundant, it is the same object as the input!
        element.add_data(raw_)

    def finalize(self):
        self.__logger.info("Completed Profiling")
        # summary = self.jobops.meta.summary
