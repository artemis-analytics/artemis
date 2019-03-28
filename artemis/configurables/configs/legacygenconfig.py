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

from artemis.tools.mftool import MfTool


@iterable
class LegacyOptions:
    generator_type = 'legacy'
    filehandler_type = 'legacy'
    nbatches = 10
    num_rows = 10000
    delimiter = '\r\n'
    nrecords_per_block = 4095


@Logger.logged
class LegacyGenConfig(Configurable):

    def __init__(self, menu=None, **kwargs):
        options = dict(GlobalConfigOptions())
        options.update(dict(LegacyIOOptions()))
        options.update(kwargs)
        super().__init__(menu, **options)

    def configure(self, **columns):

        self.__logger.info(columns)
        mftool = MfTool('legacytool', **columns)
        blocksize = mftool.record_size * self.nrecords_per_block
        rsize = 0
        schema = []
        for key in columns:
            rsize = rsize + columns[key]['length']
            schema.append(key)

        self._config_generator(nbatches=self.nbatches,
                               num_rows=self.num_rows,
                               seed=self.seed,
                               **columns)

        self._config_filehandler(blocksize=blocksize,
                                 delimiter=self.delimiter,
                                 header_offset=rsize,
                                 footer_size=rsize,
                                 schema=schema,
                                 seed=self.seed)

        # Ensure block_size for arrow parser greater than
        # file chunk size

        mftoolmsg = mftool.to_msg()
        self.__logger.info(mftoolmsg)

        self._tools.append(mftoolmsg)
        self._config_sampler()
        self._config_writer()
        self._add_tools()
        self.__logger.info(self._msg)


@iterable
class LegacyIOOptions:
    generator_type = 'file'
    filehandler_type = 'legacy'
    nbatches = 10
    num_rows = 10000
    nrecords_per_block = 4095


@Logger.logged
class LegacyIOConfig(Configurable):

    def __init__(self, menu=None, **kwargs):
        options = dict(GlobalConfigOptions())
        options.update(dict(LegacyIOOptions()))
        options.update(kwargs)
        super().__init__(menu, **options)

    def configure(self, **columns):

        self._config_generator(path=self.input_repo,
                               glob=self.input_glob,
                               nbatches=self.nbatches,
                               seed=self.seed)

        mftool = MfTool('legacytool', **columns)
        blocksize = mftool.record_size * self.nrecords_per_block
        rsize = 0
        schema = []
        for key in columns:
            rsize = rsize + columns[key]['length']
            schema.append(key)

        self._config_filehandler(blocksize=blocksize,
                                 delimiter=self.delimiter,
                                 header_offset=rsize,
                                 footer_size=rsize,
                                 schema=schema,
                                 seed=self.seed)

        # Ensure block_size for arrow parser greater than
        # file chunk size
        self._tools.append(mftool.to_msg())
        self._config_sampler()
        self._config_writer()
        self._add_tools()
        self.__logger.info(self._msg)
