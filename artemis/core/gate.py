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
Framework-level services and helper mixin classes to provide access to metadata,
histograms, timers, and stores
"""
from artemis.logger import Logger
from artemis.core.singleton import Singleton
from artemis.core.tree import Tree
from cronus.core.book import ArtemisBook, TDigestBook
from cronus.core.cronus import BaseObjectStore
from artemis_format.pymodels.cronus_pb2 import (
    HistsObjectInfo,
    TDigestObjectInfo,
    JobObjectInfo,
)
from artemis_format.pymodels.artemis_pb2 import JobInfo as JobInfo_pb
from artemis_format.pymodels.artemis_pb2 import JOB_SUCCESS
from artemis_format.pymodels.configuration_pb2 import Configuration
from artemis_format.pymodels.menu_pb2 import Menu
from cronus.core.book import ToolStore
from artemis.core.datastore import ArrowSets


@Logger.logged
class ArtemisGateSvc(metaclass=Singleton):
    """
    Wrapper class as Singleton type
    Framework level service
    Providing access to common data sinks required
    in artemis and algorithms

    Attributes
    ----------
    meta : JobInfo
        JobInfo object holding uuids to configuration and menu metadata
    hbook : ArtemisBook
        OrderedDict of all histograms in framework
    tbook : TDigestBook
        OrderedDict of all tdigests in framework
    menu : Menu
        Business process graph
    config : Configuration
        All configuration meta, including properties of tools and algorithms
    tools : ToolStore
        OrderedDict of all tool objects in framework.
    store : BaseObjectStore
        Metadata service and access to underlying data store
    tree : Tree
        Execution graph

    """

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
        """Configure the gate with jobinfo passed to Artemis.

        """
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
            self.store = BaseObjectStore(
                self.meta.store_path, self.meta.store_name, self.meta.store_id
            )
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
        duration.seconds = self.meta.finished.seconds - self.meta.started.seconds
        duration.nanos = self.meta.finished.nanos - self.meta.started.nanos
        if duration.seconds < 0 and duration.nanos > 0:
            duration.seconds += 1
            duration.nanos -= 1000000000
        elif duration.seconds > 0 and duration.nanos < 0:
            duration.seconds -= 1
            duration.nanos += 1000000000

    def _finalize_timers(self):

        for key in self.hbook.keys():
            if "time" not in key:
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
        self.__logger.info(
            "Total job time %i seconds", self.meta.summary.job_time.seconds
        )
        self.__logger.info("Processed file summary")

        self.__logger.info(
            "Total datums requested %i", self.meta.summary.processed_ndatums
        )
        self.__logger.info(
            "Total bytes processed %i", self.meta.summary.processed_bytes
        )

        self.__logger.debug("Timer Summary")
        for t in self.meta.summary.timers:
            self.__logger.debug("%s: %2.2f +/- %2.2f", t.name, t.time, t.std)
        self.__logger.info("This is a test of your greater survival")

        self.__logger.info("Processed data summary")
        self.__logger.info(
            "Mean payload %2.2f MB", self.hbook["artemis.payload"].mean()
        )
        self.__logger.info(
            "Mean blocksize %2.2f MB", self.hbook["artemis.blocksize"].mean()
        )
        self.__logger.info("Mean n block %2.2f ", self.hbook["artemis.nblocks"].mean())

        self.__logger.info("=================================")

    def finalize(self):
        """Finalize job.
        Collects timers and registers remaining content, such as histograms to the
        store.
        """
        try:
            self._finalize_timers()
        except Exception:
            self.__logger.error("Cannot finalize timers")
            raise

        hinfo = HistsObjectInfo()
        hinfo.keys.extend(self.hbook.keys())
        hmsg = self.hbook._to_message()
        try:
            self.store.register_content(
                hmsg, hinfo, dataset_id=self.meta.dataset_id, job_id=self.meta.job_id
            ).uuid
        except Exception:
            self.__logger.error("Unable to register hist")
            raise

        tinfo = TDigestObjectInfo()
        tinfo.keys.extend(self.tbook.keys())
        tmsg = self.tbook._to_message()
        try:
            self.store.register_content(
                tmsg, tinfo, dataset_id=self.meta.dataset_id, job_id=self.meta.job_id
            ).uuid
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
            self.store.register_content(
                self.meta,
                jinfo,
                dataset_id=self.meta.dataset_id,
                job_id=self.meta.job_id,
            ).uuid
        except Exception:
            self.__logger.error("Unable to register job meta data")
            raise

        self._job_report()
        # try:
        #    self.store.save_store()
        # except Exception:
        #    self.__logger.error("Error persisting metastore")
        #    raise


class MetaMixin:
    """
    Methods for setting / getting job attributes
    Meta data required in ArtemisGate to track processing
    """

    @property
    def job_id(self):
        """UUID of job
        """
        return self.gate.meta.job_id

    @property
    def path(self):
        """Absolute path to datastore, e.g. where things are written to
        """
        return self.gate.meta.store_path

    @property
    def store_name(self):
        """Name of metastore
        """
        return self.gate.meta.store_name

    @property
    def store_uuid(self):
        """UUID of store
        """
        return self.gate.meta.store_id

    @property
    def menu_id(self):
        """ UUID of menu metadata
        """
        return self.gate.meta.menu_id

    @property
    def config_id(self):
        """UUID of configuration metadata
        """
        return self.gate.meta.config_id

    @property
    def input_id(self):
        """Parent dataset uuid.
        """
        return self.gate.meta.parentset_id

    @property
    def output_id(self):
        """Output dataset uuid
        """
        return self.gate.meta.dataset_id

    def get_tool(self, name):
        """Retrieve a tool via the name
        """
        return self.gate.tools.get(name)


class IOMetaMixin:
    @property
    def job_state(self):
        """
        Job state currently stored in protobuf
        """
        return self.gate.meta.state

    @property
    def current_file(self):
        """
        current file UUID held in gate
        """
        return self.gate._current_file_id

    @property
    def processed_ndatums(self):
        """
        current number of datums processed
        """
        return self.gate.meta.summary.processed_ndatums

    @property
    def processed_bytes(self):
        """
        current total bytes processed
        """
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
        """
        register a log file in the store

        Parameters
        ----------

        Returns
        -------
            logobj : Context object
        """

        logobj = self.gate.store.register_log(
            self.gate.meta.dataset_id, self.gate.meta.job_id
        )
        return logobj

    def register_content(self, buf, info, dataset_id, job_id, partition_key=None):
        """
        register content in the store

        Parameters
        ----------
            buf : protobuf or pyarrow.buffer
                raw bytes
            info : Context object
                metadata associated to data object
            dataset_id : UUID
                UUID of dataset to associate with content
            job_id : UUID
                UUID of current job that produced data object
            partition_key : str
                partition name in dataset
        """

        return self.gate.store.register_content(
            buf, info, dataset_id=dataset_id, job_id=job_id, partition_key=partition_key
        )

    def set_file_size_bytes(self, filepath_or_buffer, size_):
        """
        set the size in bytes of an object in the context object
        """
        self.gate.store[filepath_or_buffer].file.size_bytes = size_

    def set_file_blocks(self, filepath_or_buffer, blocks):
        """
        set blocks, chunks or record batches in a file.
        provides context of how a raw data object is split, read and processed.

        Parameters
        ----------
        filepath_or_buffer : bytes
            data object
        blocks : List
            offset is zeroth element
            length is first element
        """
        # Record the blocks chunked from input datum
        for i, block in enumerate(blocks):
            msg = self.gate.store[filepath_or_buffer].file.blocks.add()
            msg.index = i
            msg.info.offset = block[0]
            msg.info.size_bytes = block[1]

    def new_partition(self, key):
        """
        add a partition to the current dataset

        Parameters
        ----------

        key : str
            name of partition. Typically a leaf name in the menu

        """
        self.gate.store.new_partition(self.gate.meta.dataset_id, key)

    def reset_job_summary(self):
        """
        clears the summary information. Typically used when sampling data.
        """
        self.gate.meta.summary.processed_bytes = 0
        self.gate.meta.summary.processed_ndatums = 0

    def get_leaves(self):
        """
        Return leaves from the execution tree
        """
        return self.gate.tree.leaves

    def get_node(self, node_id):
        """
        Return node from execution tree

        Parameters
        ----------
        node_id : str
            unique name of node
        """
        # Node name is constructed in Steering
        # See steering._element_name
        return self.gate.tree.get_node_by_key(node_id)

    def persist_to_storage(self, obj_id, buf):
        """
        Writes data object to disk via the store
        """
        self.gate.store.put(obj_id, buf)

    def datastore_flush(self):
        """
        Clears all nodes in the execution tree
        """
        self.gate.tree.flush()

    def datastore_is_empty(self):
        """
        checks that the in-memory datastore is empty

        Returns
        -------
            bool
        """
        return ArrowSets().is_empty()
