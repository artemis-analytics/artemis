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

from artemis.configurables.configurable import Configurable
from artemis.tools.csvtool import CsvTool


@Logger.logged
class CsvGenConfig(Configurable):

    def __init__(self, menu=None, max_malloc=2147483648):
        super().__init__(menu, max_malloc)

    def configure(self,
                  ctype='csv',
                  nbatches=10,
                  num_cols=20,
                  num_rows=10000,
                  blocksize=2**16,
                  delimiter='\r\n'):

        self._config_generator(ctype,
                               nbatches=nbatches,
                               num_cols=num_cols,
                               num_rows=num_rows)

        self._config_filehandler(ctype,
                                 blocksize=blocksize,
                                 delimiter=delimiter)

        # Ensure block_size for arrow parser greater than
        # file chunk size
        csvtool = CsvTool('csvtool', block_size=2**24)
        self._tools.append(csvtool.to_msg())
        self._config_sampler()
        self._config_writer()
        self._add_tools()
        self.__logger.info(self._msg)


@Logger.logged
class CsvIOConfig(Configurable):

    def __init__(self, menu=None, max_malloc=2147483648):
        super().__init__(menu, max_malloc)

    def configure(self,
                  ctype='csv',
                  nbatches=0,
                  num_cols=20,
                  num_rows=10000,
                  blocksize=2**16,
                  delimiter='\r\n',
                  path='/tmp',
                  glob='*.csv'):

        self._config_generator('file',
                               path=path,
                               glob=glob,
                               nbatches=nbatches)

        self._config_filehandler(ctype,
                                 blocksize=blocksize,
                                 delimiter=delimiter)

        # Ensure block_size for arrow parser greater than
        # file chunk size
        csvtool = CsvTool('csvtool', block_size=2**24)
        self._tools.append(csvtool.to_msg())
        self._config_sampler()
        self._config_writer()
        self._add_tools()
        self.__logger.info(self._msg)
