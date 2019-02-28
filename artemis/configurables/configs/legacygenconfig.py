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

from artemis.tools.mftool import MfTool


@Logger.logged
class LegacyGenConfig(Configurable):

    def __init__(self, menu=None, max_malloc=2147483648):
        super().__init__(menu, max_malloc)

    def configure(self,
                  ctype='legacy',
                  nbatches=10,
                  num_rows=10000,
                  delimiter='\r\n',
                  **columns):

        self.__logger.info(columns)
        mftool = MfTool('legacytool', **columns)
        blocksize = mftool.record_size * 100
        rsize = 0
        for key in columns:
            rsize = rsize + columns[key]['length']
        self._config_generator(ctype,
                               nbatches=nbatches,
                               num_rows=num_rows,
                               **columns)

        self._config_filehandler(ctype,
                                 blocksize=blocksize,
                                 delimiter=delimiter,
                                 offset_header=rsize)

        # Ensure block_size for arrow parser greater than
        # file chunk size

        mftoolmsg = mftool.to_msg()
        self.__logger.info(mftoolmsg)

        self._tools.append(mftoolmsg)
        self._config_sampler()
        self._config_writer()
        self._add_tools()
        self.__logger.info(self._msg)


@Logger.logged
class LegacyIOConfig(Configurable):

    def __init__(self, menu=None, max_malloc=2147483648):
        super().__init__(menu, max_malloc)

    def configure(self,
                  ctype='legacy',
                  nbatches=0,
                  delimiter='\r\n',
                  path='/tmp',
                  glob='*.txt',
                  nrecords_per_block=4095,
                  max_file_size=1073741824,
                  write_csv=True,
                  **columns):

        self._config_generator('file',
                               path=path,
                               glob=glob,
                               nbatches=nbatches)

        mftool = MfTool('legacytool', **columns)
        blocksize = mftool.record_size * nrecords_per_block
        rsize = 0
        for key in columns:
            rsize = rsize + columns[key]['length']
        self._config_filehandler(ctype,
                                 blocksize=blocksize,
                                 delimiter=delimiter,
                                 offset_header=rsize)

        # Ensure block_size for arrow parser greater than
        # file chunk size
        self._tools.append(mftool.to_msg())
        self._config_sampler()
        self._config_writer(max_file_size, write_csv)
        self._add_tools()
        self.__logger.info(self._msg)
