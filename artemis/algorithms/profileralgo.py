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

import pandas as pd
import numpy as np

from tdigest import TDigest
from artemis.decorators import timethis
from artemis.core.tool import ToolStore
from artemis.core.algo import AlgoBase
from artemis.core.properties import JobProperties
from artemis.core.physt_wrapper import Physt_Wrapper


class ProfilerAlgo(AlgoBase):

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.__logger.info('%s: __init__ ProfilerAlgo' % self.name)
        self.__tools = ToolStore()
        self.reader = None
        self.jobops = None
        self.digests = {}
        self.digests_initalized = False
        self.print_percentiles = True

    def initialize(self):
        self.__logger.info('%s: Initialized ProfilerAlgo' % self.name)

    def book(self):
        pass

    def create_tdigests(self, record_batch):
        tool_digests = {}
        try:
            tool_digests = self.__tools.get('tdigesttool').execute(record_batch)
            return tool_digests
        except Exception:
            raise
        return

    def execute(self, record_batch):

        raw_ = record_batch.get_data()

        # Code for understanding the record batch that this algorithim has recived
        # We can break up the record batch into a bunch of columns each column can be passed to the profiler algorithim

        self.__logger.debug('Num cols: %s Num rows: %s',
                            raw_.num_columns, raw_.num_rows)

        # Create the map column name -> TDigest from the record batch schema 
        # We only do this the first time that this algorithim is called, otherwise we continue

        if not self.digests_initalized:
            batch_schema = raw_.schema
            batch_schema_names = raw_.schema.names
            batch_columns = raw_.columns

            for i in range(len(batch_columns)):
                digest = TDigest()
                self.digests[batch_schema_names[i]] = digest
            
            self.digests_initalized = True

        # We now calculate the TDigests using the TDigest tool 
        # Declare a list of TDigest objects that will be returned by the tdigest tool
        # We pass the record batch directly to the tdigesttool

        try:
            # This too should return a map column name -> TDigest as per the record batch schema

            tool_digests = self.create_tdigests(raw_)

            keys = tool_digests.keys() & self.digests.keys()

            for key in keys:
                self.digests[key] = self.digests[key] + tool_digests[key]

        except Exception:
            self.__logger.error('TDigest creation fails')
            raise

        # Redundant, it is the same object as the input!
        #element.add_data(raw_)

    def finalize(self):
        self.__logger.info("Completed Profiling")
        #print(self.digests)
        for key, value in self.digests.items():
            #print(key, value)
            self._jp.tbook[key] = value
            #if key == 'Normal':
                #print(value.centroids_to_list())
            if len(value.centroids_to_list()) == 0:
                self.__logger.warning(key + " is not a numeric value and does not have a TDigest")
            else:
                '''
                print("Column name: " + key)
                if self.print_percentiles:
                    for i in range(100):
                        print(key + " " + str(i) + ": " + str(value.percentile(i)))
                '''
