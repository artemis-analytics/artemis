#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

from artemis.logger import Logger
from artemis.core.singleton import Singleton
from artemis.core.tree import Tree
from artemis.core.book import ArtemisBook, TDigestBook
from artemis.meta.cronus import BaseObjectStore
from artemis.io.protobuf.cronus_pb2 import HistsObjectInfo, \
    TDigestObjectInfo, JobObjectInfo
from artemis.io.protobuf.artemis_pb2 import JobInfo as JobInfo_pb
from artemis.io.protobuf.configuration_pb2 import Configuration
from artemis.io.protobuf.menu_pb2 import Menu
from artemis.core.tool import ToolBase


class ToolStore():
    # TODO
    # Check for existence of tool
    # Use dict class functionality, i.e. derive from dict

    def __init__(self):
        self.tools = {}

    def add(self, logger, toolcfg):
        # add tool from a config
        self.tools[toolcfg.name] = ToolBase.from_msg(logger, toolcfg)

    def get(self, key):
        return self.tools[key]


@Logger.logged
class ArtemisGateSvc(metaclass=Singleton):
    '''
    Wrapper class as Singleton type
    Framework level service
    Providing access to common data sinks required
    in artemis and algorithms

    MetaData: JobInfo
    Histograms: ArtemisBook
    TDigests: TDigestBook
    Job Menu: Menu
    Job Configuration: Configuration
    Tools: ToolStore
    MetaData Store: Cronus
    Steering Data Dependency Tree: Tree

    '''
    def __init__(self):
        self.meta = JobInfo_pb()
        self.hbook = ArtemisBook()
        self.tbook = TDigestBook()
        self.menu = Menu()
        self.config = Configuration()
        self.tools = ToolStore()
        self.store = None
        self.tree = None
        self._current_file_id = None

    @property
    def job_id(self):
        return self.meta.job_id

    @property
    def path(self):
        return self.meta.store_path

    @property
    def store_name(self):
        return self.meta.store_name

    @property
    def store_uuid(self):
        return self.meta.store_id

    @property
    def menu_id(self):
        return self.meta.menu_id

    @property
    def config_id(self):
        return self.meta.config_id

    @property
    def input_id(self):
        '''
        Parent dataset uuid
        '''
        return self.meta.parentset_id

    @property
    def output_id(self):
        '''
        Output dataset uuid
        '''
        return self.meta.dataset_id

    @property
    def job_state(self):
        return self.meta.state

    @job_state.setter
    def job_state(self, value):
        self.meta.state = value

    @property
    def current_file(self):
        return self._current_file_id

    @current_file.setter
    def current_file(self, value):
        self._current_file_id = value

    def configure(self, jobinfo):
        try:
            self.meta.CopyFrom(jobinfo)
        except Exception:
            self.__logger.info("Fail to copy job info message")
            raise

        # Initialize summary info in meta data
        self.meta.started.GetCurrentTime()
        self.meta.summary.processed_bytes = 0
        self.meta.summary.processed_ndatums = 0

        try:
            self.store = BaseObjectStore(self.meta.store_path,
                                         self.meta.store_name,
                                         self.meta.store_id)
        except FileNotFoundError:
            self.__logger.error("Store path does not exist")
            raise
        except Exception:
            self.__logger.error("Unknown store error")
            raise

        try:
            self.store.get(self.meta.menu_id, self.menu)
        except Exception:
            self.__logger.error("Cannot retrieve menu")
            raise
        try:
            self.store.get(self.meta.config_id, self.config)
        except Exception:
            self.__logger.error("Cannot retrieve menu")
            raise

        self.tree = Tree(self.meta.name)

    def finalize(self):

        self.__logger.info("Job Summary")
        self.__logger.info("=================================")
        self.__logger.info("Job %s", self.meta.name)
        self.__logger.info("Job id %s", self.meta.job_id)
        self.__logger.info("Total job time %i seconds",
                           self.meta.summary.job_time.seconds)
        self.__logger.info("Processed file summary")

        self.__logger.info("Total datums requested %i",
                           self.meta.summary.processed_ndatums)
        self.__logger.info("Total bytes processed %i",
                           self.meta.summary.processed_bytes)

        self.__logger.debug("Timer Summary")
        for t in self.meta.summary.timers:
            self.__logger.debug("%s: %2.2f +/- %2.2f", t.name, t.time, t.std)
        self.__logger.info("This is a test of your greater survival")
        self.__logger.info("=================================")

        jinfo = JobObjectInfo()
        try:
            self.store.register_content(self.meta,
                                        jinfo,
                                        dataset_id=self.meta.dataset_id,
                                        job_id=self.meta.job_id).uuid
        except Exception:
            self.__logger.error("Unable to register hist")
            raise

        hinfo = HistsObjectInfo()
        hinfo.keys.extend(self.hbook.keys())
        hmsg = self.hbook._to_message()
        try:
            self.store.register_content(hmsg,
                                        hinfo,
                                        dataset_id=self.meta.dataset_id,
                                        job_id=self.meta.job_id).uuid
        except Exception:
            self.__logger.error("Unable to register hist")
            raise

        tinfo = TDigestObjectInfo()
        tinfo.keys.extend(self.tbook.keys())
        tmsg = self.tbook._to_message()
        try:
            self.store.register_content(tmsg,
                                        tinfo,
                                        dataset_id=self.meta.dataset_id,
                                        job_id=self.meta.job_id).uuid
        except Exception:
            self.__logger.error("Unable to register hist")
            raise
        # try:
        #    self.store.save_store()
        # except Exception:
        #    self.__logger.error("Error persisting metastore")
        #    raise


class StoreMixin():
    '''
    Methods for interacting with Cronus BaseObjectStore
    '''

    def register_content(self, buf,
                         info, dataset_id,
                         job_id, partition_key=None):

        return self.gate.store.register_content(buf, info,
                                                dataset_id=dataset_id,
                                                job_id=job_id,
                                                partition_key=partition_key)


class MetaMixin():
    '''
    Methods for setting / getting job attributes
    '''
    pass


class ToolStoreMixin():
    '''
    Mixin class for interacting with the ToolStore
    via ArtemisGateSvc in framework algorithms
    '''

    def get_tool(self, name):
        return self.gate.tools.get(name)
