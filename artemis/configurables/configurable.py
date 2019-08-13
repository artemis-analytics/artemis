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
from artemis.decorators import iterable
from artemis.io.writer import BufferOutputWriter
from artemis.configurables.factories import GeneratorFactory
from artemis.configurables.factories import FileHandlerFactory

from artemis.tools.tdigesttool import TDigestTool

from artemis.io.protobuf.configuration_pb2 import Configuration
from artemis.meta.Directed_Graph import GraphMenu


@iterable
class GlobalConfigOptions:
    '''
    Options required to specify a dataset job
    Each instance of Artemis must be configured with these options

    Options set to None should be specified as default options
    in inherited configurations
    or must be set by the user
    '''
    # Required
    jobname = None  # Common to all jobs using the same menu and configuration
    output_repo = None  # Absolute path to write output
    dbkey = None  # Unique key to store/retrieve the configuration from the DB

    # Optional is using filegenerator
    input_repo = None  # Absolute path to dataset
    input_glob = None

    # Defaults
    max_malloc = 2147483648  # Maximum memory allowed in Arrow memory pool
    max_buffer_size = 2147483648  # Maximum size serialized ipc message
    write_csv = True  # Output csv files
    sample_ndatums = 1  # Preprocess job to sample files from dataset
    sample_nchunks = 10  # Preprocess job to sample chunks from a file
    loglevel = 'INFO'
    # Set by the config classes
    generator_type = None
    filehandler_type = None
    seed = None


@Logger.logged
class Configurable():

    def __init__(self, menu, **options):
        '''
        Menu required as input
        '''
        if menu is None:
            raise ValueError

        # add attribute from options
        for name, value in options.items():
            setattr(self, name, value)

        if self.dbkey:
            self._msg = self.retrieve_from_db()
        else:
            # self._msg = artemis_pb2.JobConfig()
            self._msg = Configuration()
            self._msg.uuid = str(uuid.uuid4())
            self._msg.name = f"{self._msg.uuid}.config.pb"

        self._msg.max_malloc_size_bytes = self.max_malloc
        self._tools = []

        if hasattr(self._msg, 'config_id'):
            self._msg.config_id = str(uuid.uuid4())
            self.__logger.info('Job configuration uuid %s',
                               self._msg.config_id)
        # if menu:
        #    self._msg.menu.CopyFrom(menu)

    @property
    def job_config(self):
        return self._msg

    def retrieve_from_db(self):
        '''
        Create DB connection
        Not required to run full configuration
        completes the job configuration process
        '''
        pass

    def configure(self):
        pass

    def _config_generator(self, **kwargs):
        '''
        ctype = configuration class to generate
            csv -- generate csv-like data
            legacy -- generator legacy cp500 data
            file -- generator of files

        kwargs specified in inherited job configurables
        '''
        self.__logger.info(kwargs)
        generator = GeneratorFactory(self.generator_type, **kwargs)
        self._msg.input.generator.config.CopyFrom(generator.to_msg())

    def _config_tdigest(self, **kwargs):
        '''
        creates the tdigest tool for tests

        the **kwargs are not used in this configuration setup
        '''
        tdigesttool = TDigestTool('tdigesttool')
        self._tools.append(tdigesttool.to_msg())

    def _config_filehandler(self, **kwargs):
        '''
        ctype = configuration class to generator
            accepted class types:
            csv -- reads csv data
            legacy -- reads legacy cp500 data

        kwargs specified in inherited job configurables
        '''
        tool = FileHandlerFactory(self.filehandler_type, **kwargs)
        self._tools.append(tool.to_msg())

    def _config_writer(self):
        self.__logger.info("Configure writer")
        self.__logger.info("Max file size %i", self.max_buffer_size)
        self.__logger.info("Write csv %s", self.write_csv)
        self.__logger.info("Absolute output path %s", self.output_repo)
        tool = BufferOutputWriter('bufferwriter',
                                  BUFFER_MAX_SIZE=self.max_buffer_size,
                                  write_csv=self.write_csv,
                                  path=self.output_repo)
        self._tools.append(tool.to_msg())

    def _config_sampler(self):
        sampler = self._msg.sampler
        sampler.ndatums = self.sample_ndatums
        sampler.nchunks = self.sample_nchunks

    def _add_tools(self):
        for tool in self._tools:
            msg = self._msg.tools.add()
            msg.CopyFrom(tool)

    def add_algos(self, algos):
        '''
        algos : dict of algos from MenuBuilder
        '''
        for key in algos:
            msg = self._msg.algos.add()
            msg.CopyFrom(algos[key].to_msg())


@Logger.logged
class MenuBuilder():
    '''
    Standard method to build menus
    Menus can be stored in a DB
    So, should be retrieved via a key and converted to
    the protobuf
    '''

    def __init__(self, name='test'):
        self._name = name
        self._algos = dict()
        self._seqs = dict()
        self._chains = dict()

    @property
    def algos(self):
        return self._algos

    def _algo_builder(self):
        pass

    def _seq_builder(self):
        pass

    def _chain_builder(self):
        pass

    def build(self):
        menu = GraphMenu(self._name)
        self._algo_builder()
        self._seq_builder()
        self._chain_builder()
        for chain in self._chains:
            self._chains[chain].build()
            menu.add(self._chains[chain])
        menu.build()

        return menu.to_msg()
