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
from artemis.io.protobuf.artemis_pb2 import JOB_SUCCESS
from artemis.io.protobuf.configuration_pb2 import Configuration
from artemis.io.protobuf.menu_pb2 import Menu
from artemis.core.book import ToolStore
from artemis.core.datastore import ArrowSets


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

    def _finalize_jobstate(self):
        self.meta.state = JOB_SUCCESS
        self.meta.finished.GetCurrentTime()
        duration = self.meta.summary.job_time
        duration.seconds = self.meta.finished.seconds -\
            self.meta.started.seconds
        duration.nanos = self.meta.finished.nanos -\
            self.meta.started.nanos
        if duration.seconds < 0 and duration.nanos > 0:
            duration.seconds += 1
            duration.nanos -= 1000000000
        elif duration.seconds > 0 and duration.nanos < 0:
            duration.seconds -= 1
            duration.nanos += 1000000000

    def _finalize_timers(self):

        for key in self.hbook.keys():
            if 'time' not in key:
                continue
            mu = self.hbook[key].mean()
            std = self.hbook[key].std()

            # Add to the msg
            msgtime = self.meta.summary.timers.add()
            msgtime.name = key
            msgtime.time = mu
            msgtime.std = std

    def _job_report(self):
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

        self.__logger.info("Processed data summary")
        self.__logger.info("Mean payload %2.2f MB",
                           self.hbook['artemis.payload'].mean())
        self.__logger.info("Mean blocksize %2.2f MB",
                           self.hbook['artemis.blocksize'].mean())
        self.__logger.info("Mean n block %2.2f ",
                           self.hbook['artemis.nblocks'].mean())

        self.__logger.info("=================================")

    def finalize(self):

        try:
            self._finalize_timers()
        except Exception:
            self.__logger.error("Cannot finalize timers")
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
            self.__logger.error("Unable to register tdigest")
            raise

        try:
            self._finalize_jobstate()
        except Exception:
            self.__logger.error("Cannot finalize job state and timer")
            raise

        jinfo = JobObjectInfo()
        try:
            self.store.register_content(self.meta,
                                        jinfo,
                                        dataset_id=self.meta.dataset_id,
                                        job_id=self.meta.job_id).uuid
        except Exception:
            self.__logger.error("Unable to register job meta data")
            raise

        self._job_report()
        # try:
        #    self.store.save_store()
        # except Exception:
        #    self.__logger.error("Error persisting metastore")
        #    raise


class MetaMixin():
    '''
    Methods for setting / getting job attributes
    Meta data required in ArtemisGate to track processing
    '''
    @property
    def job_id(self):
        return self.gate.meta.job_id

    @property
    def path(self):
        return self.gate.meta.store_path

    @property
    def store_name(self):
        return self.gate.meta.store_name

    @property
    def store_uuid(self):
        return self.gate.meta.store_id

    @property
    def menu_id(self):
        return self.gate.meta.menu_id

    @property
    def config_id(self):
        return self.gate.meta.config_id

    @property
    def input_id(self):
        '''
        Parent dataset uuid
        '''
        return self.gate.meta.parentset_id

    @property
    def output_id(self):
        '''
        Output dataset uuid
        '''
        return self.gate.meta.dataset_id

    def get_tool(self, name):
        return self.gate.tools.get(name)


class IOMetaMixin():

    @property
    def job_state(self):
        return self.gate.meta.state

    @property
    def current_file(self):
        return self.gate._current_file_id

    @property
    def processed_ndatums(self):
        return self.gate.meta.summary.processed_ndatums

    @property
    def processed_bytes(self):
        return self.gate.meta.summary.processed_bytes

    @job_state.setter
    def job_state(self, value):
        self.gate.meta.state = value

    @current_file.setter
    def current_file(self, value):
        self.gate._current_file_id = value

    @processed_ndatums.setter
    def processed_ndatums(self, value):
        self.gate.meta.summary.processed_ndatums += value

    @processed_bytes.setter
    def processed_bytes(self, value):
        self.gate.meta.summary.processed_bytes += value

    def register_log(self):
        logobj = self.gate.store.register_log(self.gate.meta.dataset_id,
                                              self.gate.meta.job_id)
        return logobj

    def register_content(self, buf,
                         info, dataset_id,
                         job_id, partition_key=None):

        return self.gate.store.register_content(buf, info,
                                                dataset_id=dataset_id,
                                                job_id=job_id,
                                                partition_key=partition_key)

    def set_file_size_bytes(self, filepath_or_buffer, size_):
        self.gate.store[filepath_or_buffer].file.size_bytes = size_

    def set_file_blocks(self, filepath_or_buffer, blocks):
        # Record the blocks chunked from input datum
        for i, block in enumerate(blocks):
            msg = self.gate.store[filepath_or_buffer].file.blocks.add()
            msg.index = i
            msg.info.offset = block[0]
            msg.info.size_bytes = block[1]

    def new_partition(self, key):
        self.gate.store.new_partition(self.gate.meta.dataset_id, key)

    def reset_job_summary(self):
        self.gate.meta.summary.processed_bytes = 0
        self.gate.meta.summary.processed_ndatums = 0

    def get_leaves(self):
        return self.gate.tree.leaves

    def get_node(self, node_id):
        return self.gate.tree.get_node_by_key(node_id)

    def persist_to_storage(self, obj_id, buf):
        self.gate.store.put(obj_id, buf)

    def datastore_flush(self):
        self.gate.tree.flush()

    def datastore_is_empty(self):
        return ArrowSets().is_empty()
