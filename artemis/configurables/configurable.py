#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Base class for creating a job configuration
"""
import uuid

from artemis.logger import Logger
import artemis.io.protobuf.artemis_pb2 as artemis_pb2

from artemis.io.writer import BufferOutputWriter
from artemis.configurables.factories import GeneratorFactory
from artemis.configurables.factories import FileHandlerFactory
from artemis.core.dag import Menu


@Logger.logged
class Configurable():

    def __init__(self, menu=None, max_malloc=2147483648, loglevel='INFO'):
        self._msg = artemis_pb2.JobConfig()
        self._msg.max_malloc_size_bytes = max_malloc
        self._tools = []

        if hasattr(self._msg, 'config_id'):
            self._msg.config_id = str(uuid.uuid4())
            self.__logger.info('Job configuration uuid %s',
                               self._msg.config_id)
        if menu:
            self._msg.menu.CopyFrom(menu)

    @property
    def job_config(self):
        return self._msg

    def configure(self):
        pass

    def _config_generator(self, ctype, **kwargs):
        generator = GeneratorFactory(ctype, **kwargs)
        self._msg.input.generator.config.CopyFrom(generator.to_msg())

    def _config_filehandler(self, ctype, **kwargs):
        tool = FileHandlerFactory(ctype, **kwargs)
        self._tools.append(tool.to_msg())

    def _config_writer(self, max_size=10485760, write_csv=True):

        tool = BufferOutputWriter('bufferwriter',
                                  BUFFER_MAX_SIZE=max_size,
                                  write_csv=write_csv)
        self._tools.append(tool.to_msg())

    def _config_sampler(self):
        sampler = self._msg.sampler
        sampler.ndatums = 0
        sampler.nchunks = 0

    def _add_tools(self):
        for tool in self._tools:
            msg = self._msg.tools.add()
            msg.CopyFrom(tool)


@Logger.logged
class MenuBuilder():

    def __init__(self, name='test'):
        self._name = name
        self._algos = dict()
        self._seqs = dict()
        self._chains = dict()

    def _algo_builder(self):
        pass

    def _seq_builder(self):
        pass

    def _chain_builder(self):
        pass

    def build(self):
        menu = Menu(self._name)
        self._algo_builder()
        self._seq_builder()
        self._chain_builder()
        for chain in self._chains:
            menu.add(self._chains[chain])
        menu.generate()

        return menu.to_msg()
