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
from artemis.logger import Logger
from artemis.decorators import iterable
from artemis.configurables.configurable import Configurable
from artemis.configurables.configurable import GlobalConfigOptions

from artemis.tools.mftool import MfTool
from artemis.tools.fwftool import FwfTool


@iterable
class LegacyOptions:
    generator_type = "legacy"
    filehandler_type = "legacy"
    nbatches = 10
    num_rows = 10000
    delimiter = "\r\n"
    nrecords_per_block = 4095
    encoding = "cp500"
    header = ""
    footer = ""
    skip_rows = 0
    column_names = []
    field_widths = []


@Logger.logged
class LegacyGenConfig(Configurable):
    def __init__(self, menu=None, **kwargs):
        options = dict(GlobalConfigOptions())
        options.update(dict(LegacyIOOptions()))
        options.update(kwargs)
        super().__init__(menu, **options)

    def configure(self, **columns):

        self.__logger.info(columns)
        mftool = MfTool("legacytool", **columns)
        blocksize = mftool.record_size * self.nrecords_per_block
        rsize = 0
        schema = []
        for key in columns:
            rsize = rsize + columns[key]["length"]
            schema.append(key)

        fwftool = FwfTool(
            "fwftool",
            block_size=(2 * blocksize),
            is_cobol=True,
            skip_rows=self.skip_rows,
            column_names=self.column_names,
            field_widths=self.field_widths,
            encoding=self.encoding + ",swaplfnl",
        )

        self._config_generator(
            nbatches=self.nbatches,
            num_rows=self.num_rows,
            seed=self.seed,
            header=self.header,
            footer=self.footer,
            header_offset=self.header_offset,
            footer_size=self.footer_size,
            encoding=self.encoding,
            **columns
        )

        self._config_filehandler(
            blocksize=blocksize,
            delimiter=self.delimiter,
            header=self.header,
            footer=self.footer,
            header_offset=rsize,
            footer_size=rsize,
            encoding=self.encoding,
            schema=schema,
            seed=self.seed,
        )

        self._config_tdigest()

        # Ensure block_size for arrow parser greater than
        # file chunk size

        mftoolmsg = mftool.to_msg()
        self.__logger.info(mftoolmsg)

        self._tools.append(mftoolmsg)
        self._tools.append(fwftool.to_msg())
        self._config_sampler()
        self._config_writer()
        self._add_tools()
        self.__logger.info(self._msg)


@iterable
class LegacyIOOptions:
    generator_type = "file"
    filehandler_type = "legacy"
    nbatches = 10
    num_rows = 10000
    nrecords_per_block = 4095
    encoding = "cp500"
    header = ""
    footer = ""
    skip_rows = 0
    column_names = []
    field_widths = []


@Logger.logged
class LegacyIOConfig(Configurable):
    def __init__(self, menu=None, **kwargs):
        options = dict(GlobalConfigOptions())
        options.update(dict(LegacyIOOptions()))
        options.update(kwargs)
        super().__init__(menu, **options)

    def configure(self, **columns):

        self._config_generator(
            path=self.input_repo,
            glob=self.input_glob,
            nbatches=self.nbatches,
            seed=self.seed,
        )

        mftool = MfTool("legacytool", codec=self.encoding, **columns)
        blocksize = mftool.record_size * self.nrecords_per_block

        fwftool = FwfTool(
            "fwftool",
            block_size=(2 * blocksize),
            is_cobol=True,
            skip_rows=self.skip_rows,
            column_names=self.column_names,
            field_widths=self.field_widths,
            encoding=self.encoding + ",swaplfnl",
        )
        rsize = 0
        schema = []
        for key in columns:
            rsize = rsize + columns[key]["length"]
            schema.append(key)

        self._config_filehandler(
            blocksize=blocksize,
            header=self.header,
            footer=self.footer,
            header_offset=rsize,
            footer_size=rsize,
            schema=schema,
            encoding=self.encoding,
            seed=self.seed,
        )
        self._config_tdigest()

        # Ensure block_size for arrow parser greater than
        # file chunk size
        self._tools.append(mftool.to_msg())
        self._tools.append(fwftool.to_msg())
        self._config_sampler()
        self._config_writer()
        self._add_tools()
        self.__logger.info(self._msg)
