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

"""
from pyarrow.csv import read_csv, ReadOptions, ParseOptions, ConvertOptions

from artemis.decorators import iterable
from artemis.core.tool import ToolBase


@iterable
class CsvToolOptions:

    # Add user-defined options for Artemis.CsvTool
    pass


class CsvTool(ToolBase):

    def __init__(self, name, **kwargs):

        # Retrieves the default options from arrow
        # Updates with any user-defined options
        # Create a final dictionary to store all properties
        ropts = self._get_opts(ReadOptions(), **kwargs)
        popts = self._get_opts(ParseOptions(), **kwargs)
        copts = self._get_opts(ConvertOptions(), **kwargs)
        options = {**ropts, **popts, **copts, **dict(CsvToolOptions())}
        options.update(kwargs)

        super().__init__(name, **options)
        self.__logger.info(options)
        self._readopts = ReadOptions(**ropts)
        self._parseopts = ParseOptions(**popts)
        self._convertopts = ConvertOptions(**copts)
        self.__logger.info('%s: __init__ CsvTool', self.name)
        self.__logger.info("Options %s", options)

    def _get_opts(self, cls, **kwargs):
        options = {}
        for attr in dir(cls):
            if attr[:2] != '__' and attr != "escape_char":
                options[attr] = getattr(cls, attr)
                if attr in kwargs:
                    options[attr] = kwargs[attr]
        return options

    def initialize(self):
        self.__logger.info("%s properties: %s",
                           self.__class__.__name__,
                           self.properties)

    def execute(self, block):
        '''
        Calls the read_csv module from pyarrow

        Parameters
        ----------
        block: pa.py_buffer

        Returns
        ---------
        pyarrow RecordBatch
        '''
        try:
            table = read_csv(block,
                             read_options=self._readopts,
                             parse_options=self._parseopts,
                             convert_options=self._convertopts)
        except Exception:
            self.__logger.error("Problem converting csv to table")
            raise
        # We actually want a batch
        # batch can be converted to table
        # but not vice-verse, we get batches
        # Should always be length 1 though (chunksize can be set however)
        batches = table.to_batches()
        self.__logger.debug("Batches %i", len(batches))
        for batch in batches:
            self.__logger.debug("Batch records %i", batch.num_rows)
        if len(batches) != 1:
            self.__logger.error("Table has more than 1 RecordBatches")
            raise Exception

        return batches[-1]
