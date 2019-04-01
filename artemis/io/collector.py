#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Monitor pyarrow memory pool
Tree data management
Buffer management
Requires access to metadata
"""
import pyarrow as pa

from artemis.logger import Logger
from artemis.decorators import timethis, iterable
from artemis.core.algo import AlgoBase
from artemis.core.tree import Tree
from artemis.core.datastore import ArrowSets
from artemis.core.tool import ToolStore
from artemis.core.properties import JobProperties
from google.protobuf import text_format


@iterable
class CollectorOptions:
    max_malloc = 2147483648  # Maximum memory allowed in Arrow memory pool


class Collector(AlgoBase):

    def __init__(self, name, **kwargs):
        '''
        Access the Base logger directly through
        self.__logger
        Derived class use the classmethods for info, debug, warn, error
        All formatting, loglevel checks, etc...
        can be done through the classmethods

        Can we use staticmethods in artemis to make uniform
        formatting of info, debug, warn, error?
        '''
        options = dict(CollectorOptions())
        options.update(kwargs)

        super().__init__(name, **options)
        # Configure logging
        Logger.configure(self, **kwargs)

        self.__logger.debug('__init__ Collector')

        self.max_malloc = self.properties.max_malloc
        self.job_id = self.properties.job_id
        self.path = self.properties.path

        self._jp = None
        self.tools = None
        self.hbook = None
        self.tree = None

    def initialize(self):
        self.__logger.info("initialize")
        self._jp = JobProperties()
        self.tools = ToolStore()
        self.tree = Tree()

        # Configure the output data streams
        # Each stream associated with leaf in Tree
        # Store consistent Arrow Table for each process chain
        # Currently the tree does not enforce a leaf to write out
        # record batches, so we could get spurious output buffers

        # Buffer stream requires a fixed pyArrow schema!
        _msgcfg = self._jp.meta.config
        _wrtcfg = None
        for toolcfg in _msgcfg.tools:
            if toolcfg.name == "bufferwriter":
                _wrtcfg = toolcfg

        try:
            for leaf in self.tree.leaves:
                self.__logger.info("Leave node %s", leaf)
                node = self.tree.get_node_by_key(leaf)
                key = node.key
                try:
                    _last = node.payload[-1].get_data()
                except IndexError:
                    self.__logger.error("Cannot retrieve payload! %s", key)
                    raise

                # TODO
                # Properly configure the properties in the job config
                # This is a workaround which overwrites any set properties
                if isinstance(_last, pa.lib.RecordBatch):
                    _wrtcfg.name = "writer_" + key
                    self.__logger.info("Add Tool %s", _wrtcfg.name)
                    self.tools.add(self.__logger, _wrtcfg)
                    self.tools.get(_wrtcfg.name)._schema = _last.schema
                    self.tools.get(_wrtcfg.name)._fbasename = self.job_id
                    self.tools.get(_wrtcfg.name)._path = self.path
                    self.tools.get(_wrtcfg.name).initialize()
        except Exception:
            self.__logger.error("Problem creating output streams")
            raise

        # Batches serialized, clear the tree
        try:
            Tree().flush()
        except Exception:
            self.__logger("Problem flushing")
            raise

    def book(self):
        pass

    def execute(self):
        '''
        Check total allocated memory in Arrow
        and call collect
        Collect does not ensure the file flushed
        Tuning on total allocated memory and the max output buffer
        size before spill
        '''
        if pa.total_allocated_bytes() > self.max_malloc:
            # TODO: Insert collect for datastore/nodes/tree.
            # TODO: Test memory release.
            # TODO: Add histogram for number of forced collects
            self.__logger.info("COLLECT: Total memory reached")
            try:
                result_, time_ = self._collect()
            except Exception:
                self.__logger.error("Problem collecting")
                raise
            self.__logger.info("Allocated %i", pa.total_allocated_bytes())
            self.hbook.fill('artemis', 'time.collect', time_)

    @timethis
    def _collect(self):
        '''
        Collect all batches from the leaves
        Occurs after single input source is chunked
        Each chunked converted to a batch
        Batches on leaves collected
        Input file -> Output Arrow RecordBatches
        '''
        self.__logger.info("artemis: collect: pyarrow malloc %i",
                           pa.total_allocated_bytes())

        self.__logger.info("Leaves %s", self.tree.leaves)
        for leaf in self.tree.leaves:
            self.__logger.info("Leaf node %s", leaf)
            node = self.tree.get_node_by_key(leaf)
            els = node.payload
            self.__logger.info('Batches of leaf %s', len(els))
            _name = "writer_"+node.key
            if isinstance(els[-1].get_data(), pa.lib.RecordBatch):
                self.__logger.info("RecordBatch")
                self.__logger.info("Allocated %i", pa.total_allocated_bytes())
                _schema_batch = els[-1].get_data().schema
                # TODO
                # Get the pyarrow schema as early as possible
                # Store/retrieive from the metastore
                # Do not assume each file has same schema!!!!
                try:
                    self.tools.get("writer_"+node.key)._schema = \
                        _schema_batch
                    self.tools.get("writer_"+node.key).write(els)
                    self.__logger.info("Records %i Batches %i",
                                       self.tools.get(_name)._nrecords,
                                       self.tools.get(_name)._nbatches)
                except Exception:
                    self.__logger.error("Error in buffer writer")
                    raise
            else:
                self.__logger.info("%s", type(els[-1].get_data()))

        # Batches serialized, clear the tree to flush memory
        try:
            Tree().flush()
        except Exception:
            self.__logger("Problem flushing")
            raise

        self.__logger.info("Allocated after write %i",
                           pa.total_allocated_bytes())
        self.__logger.info
        return True

    def finalize(self):
        '''
        Ensure the data store is empty
        Spill any remaining arrow buffers to disk
        '''
        # Ensure all data has been sent to buffer
        _store = ArrowSets()
        if _store.is_empty() is False:
            self.__logger.info("Collecting remaining data")
            try:
                result_, time_ = self._collect()
            except Exception:
                self.__logger.error("Problem collecting")
                raise
            self.__logger.info("Allocated %i", pa.total_allocated_bytes())
        # Spill any remaining buffers to disk
        # Set the output file metadata
        summary = self._jp.meta.summary
        _wnames = []
        for leaf in Tree().leaves:
            self.__logger.info("Leave node %s", leaf)
            node = Tree().get_node_by_key(leaf)
            key = node.key
            _wnames.append("writer_" + node.key)

        for key in _wnames:
            try:
                writer = self.tools.get(key)
            except KeyError:
                continue
            try:
                writer._finalize()
            except Exception:
                self.__logger.error("Finalize buffer stream fails %s", key)
                raise

            self.__logger.info("File summary statistics")
            for table in writer._finfo:
                tableinfo = summary.tables.add()
                tableinfo.CopyFrom(table)
                self.__logger.info(text_format.MessageToString(tableinfo))

            self.__logger.info("Dataset summary statistics")
            self.__logger.info("%s Records: %i Batches: %i Files: %i",
                               writer.name,
                               writer.total_records,
                               writer.total_batches,
                               writer.total_files)

    def _flush_buffer(self):
        _wnames = []
        for leaf in Tree().leaves:
            self.__logger.info("Leave node %s", leaf)
            node = Tree().get_node_by_key(leaf)
            key = node.key
            _wnames.append("writer_" + node.key)

        for key in _wnames:
            try:
                writer = self.tools.get(key)
            except KeyError:
                continue
            try:
                writer.flush()
            except Exception:
                self.__logger.error("Flush buffer stream fails %s", key)
                raise
