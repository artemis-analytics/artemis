#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""

"""
import io
from ast import literal_eval
import csv

import pyarrow as pa
from pyarrow.csv import read_csv, ReadOptions, ParseOptions

from artemis.core.tool import ToolBase


class CsvTool(ToolBase):

    def __init__(self, name, **kwargs):
        defaults = self._set_defaults()
        # Override the defaults from the kwargs
        for key in kwargs:
            defaults[key] = kwargs[key]
        super().__init__(name, **defaults)
        self.__logger.info(defaults)
        _ropts = self._update_opts(ReadOptions, **defaults)
        _popts = self._update_opts(ParseOptions, **defaults)
        self.__logger.info("Configured ReadOptions: ")
        self.__logger.info(_ropts)
        self.__logger.info("Configured ParseOptions ")
        self.__logger.info(_popts)
        self._readopts = ReadOptions(**_ropts)
        self._parseopts = ParseOptions(**_popts)
        self._convertopts = None  # Coming in 0.12
        self.__logger.info('%s: __init__ FileHandlerTool' % self.name)

    def _update_opts(self, cls, **kwargs):
        _updates = self._get_opts(cls())
        # Drop escape char from defaults
        if 'escape_char' in _updates.keys():
            del _updates['escape_char']
        for key in _updates:
            _updates[key] = kwargs[key]
        return _updates

    def _get_opts(self, opts):
        defaults = {}
        for attr in dir(opts):
            if attr.startswith("__"):
                continue
            defaults[attr] = getattr(opts, attr)
        return defaults

    def _set_defaults(self):
        ropts = self._get_opts(ReadOptions())  # Retrieve defaults from pyarrow
        popts = self._get_opts(ParseOptions())
        self.__logger.info("Default Read options")
        self.__logger.info(ropts)
        self.__logger.info("Default Parse options")
        self.__logger.info(popts)
        # Remove escape_char option, required to be None (False)
        del popts['escape_char']

        defaults = {**ropts, **popts}

        return defaults

    def initialize(self):
        self.__logger.info("%s properties: %s",
                           self.__class__.__name__,
                           self.properties)

    def execute_pyparsing(self, schema, columns, length, block):
        '''
        Relies on python object inspection to determine type
        Used as a comparison and validation tool for the pyarrow module
        '''
        try:
            with io.TextIOWrapper(io.BytesIO(block)) as file_:
                reader = csv.reader(file_)
                try:
                    next(reader)
                except Exception:
                    self.__logger.error("Cannot read inserted header")
                    raise
                try:
                    for row in reader:
                        # print(row)
                        length += 1
                        for i, item in enumerate(row):
                            if item == 'nan':
                                item = 'None'
                            try:
                                columns[i].append(literal_eval(item))
                            except Exception:
                                self.__logger.error("Line %i row %s" %
                                                    (i, row))
                                raise
                except Exception:
                    self.__logger.error('Error reading line %i' % length)
                    raise
        except IOError:
            raise
        except Exception:
            raise

        array = []
        for column in columns:
            try:
                array.append(pa.array(column))
            except Exception:
                self.__logger.error("Cannot convert list to pyarrow arrow")
                raise
        try:
            rbatch = pa.RecordBatch.from_arrays(array, schema)
        except Exception:
            self.__logger.error("Cannot convert arrays to batch")
            raise
        return rbatch

    def execute(self, block):
        '''
        Calls the read_csv module from pyarrow

        Parameters
        ----------
        buf: bytearray object containing raw bytes from csv file

        Returns
        ---------
        pyarrow RecordBatch
        '''
        # create pyarrow buffer from raw bytes
        buf_ = pa.py_buffer(block)
        try:
            table = read_csv(buf_,
                             read_options=self._readopts,
                             parse_options=self._parseopts)
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
