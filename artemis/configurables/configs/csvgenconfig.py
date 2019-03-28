#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""

"""
from artemis.logger import Logger
from artemis.decorators import iterable

from artemis.configurables.configurable import Configurable
from artemis.configurables.configurable import GlobalConfigOptions
from artemis.tools.csvtool import CsvTool


@iterable
class CsvGenOptions:
    generator_type = 'csv'
    filehandler_type = 'csv'
    nbatches = 10
    num_cols = 20
    num_rows = 10000
    linesep = '\r\n'
    delimiter = ","
    blocksize = 2**16


@Logger.logged
class CsvGenConfig(Configurable):

    def __init__(self, menu=None, **kwargs):
        options = dict(GlobalConfigOptions())
        options.update(dict(CsvGenOptions()))
        options.update(kwargs)
        super().__init__(menu, **options)

    def configure(self):

        self._config_generator(nbatches=self.nbatches,
                               num_cols=self.num_cols,
                               num_rows=self.num_rows,
                               seed=self.seed)

        self._config_filehandler(blocksize=self.blocksize,
                                 delimiter=self.delimiter,
                                 seed=self.seed)

        # Ensure block_size for arrow parser greater than
        # file chunk size
        csvtool = CsvTool('csvtool', block_size=(2 * self.blocksize))
        self._tools.append(csvtool.to_msg())
        self._config_sampler()
        self._config_writer()
        self._add_tools()
        self.__logger.info(self._msg)


@Logger.logged
class CsvIOConfig(Configurable):

    def __init__(self, menu=None, **kwargs):
        options = dict(GlobalConfigOptions())
        options.update(dict(CsvGenOptions()))
        options.update(kwargs)
        super().__init__(menu, **options)

    def configure(self):
        self._config_generator(path=self.input_repo,
                               glob=self.input_glob,
                               nbatches=self.nbatches,
                               seed=self.seed)

        self._config_filehandler(blocksize=self.blocksize,
                                 delimiter=self.delimiter,
                                 seed=self.seed)

        # Ensure block_size for arrow parser greater than
        # file chunk size
        csvtool = CsvTool('csvtool', block_size=(self.blocksize*2))
        self._tools.append(csvtool.to_msg())
        self._config_sampler()
        self._config_writer()
        self._add_tools()
        self.__logger.info(self._msg)
