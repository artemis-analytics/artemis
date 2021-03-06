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
Collector monitors Arrow memory-pool and manages file creation, spills to disk data
on-demand, and flushes memory pool when required.
"""
import pyarrow as pa

from artemis.logger import Logger
from artemis.decorators import timethis, iterable
from artemis.core.algo import IOAlgoBase
from artemis.core.tree import Tree
from artemis.core.datastore import ArrowSets
from google.protobuf import text_format


@iterable
class CollectorOptions:
    max_malloc = 2147483648  # Maximum memory allowed in Arrow memory pool


class Collector(IOAlgoBase):
    def __init__(self, name, **kwargs):
        """
        """
        options = dict(CollectorOptions())
        options.update(kwargs)

        super().__init__(name, **options)
        # Configure logging
        Logger.configure(self, **kwargs)

        self.__logger.debug("__init__ Collector")

        self.max_malloc = self.properties.max_malloc

    def initialize(self):
        self.__logger.info("initialize")
        # Configure the output data streams
        # Each stream associated with leaf in Tree
        # Store consistent Arrow Table for each process chain
        # Currently the tree does not enforce a leaf to write out
        # record batches, so we could get spurious output buffers

        # Buffer stream requires a fixed pyArrow schema!
        _wrtcfg = None
        try:
            _wrtcfg = self.gate.config.tools["bufferwriter"]
        except KeyError:
            self.__logger.error("BufferWriter configuration not found")
            raise

        if _wrtcfg is None:
            self.__logger.error("BufferWriter not configured")
            raise KeyError

        for leaf in self.gate.tree.leaves:
            self.__logger.info("Leave node %s", leaf)
            node = self.gate.tree.get_node_by_key(leaf)
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
                self.__logger.info("Writer config")
                self.__logger.info(_wrtcfg)
                try:
                    _wrtcfg.name = "writer_" + key
                except AttributeError:
                    self.__logger.error("BufferWriter misconfigured")
                    raise
                self.__logger.info("Add Tool %s", _wrtcfg.name)
                self.gate.tools.add(self.__logger, _wrtcfg)
                self.gate.tools.get(_wrtcfg.name)._schema = _last.schema
                self.gate.tools.get(_wrtcfg.name).initialize()
                self.gate.store.new_partition(self.gate.meta.dataset_id, key)

        # Batches serialized, clear the tree
        try:
            self.gate.tree.flush()
        except Exception:
            self.__logger("Problem flushing")
            raise

    def book(self):
        pass

    def execute(self):
        """
        Check total allocated memory in Arrow
        and call collect
        Collect does not ensure the file flushed
        Tuning on total allocated memory and the max output buffer
        size before spill
        """
        if pa.total_allocated_bytes() > self.max_malloc:
            # TODO: Insert collect for datastore/nodes/tree.
            # TODO: Test memory release.
            # TODO: Add histogram for number of forced collects
            self.__logger.debug("COLLECT: Total memory reached")
            try:
                result_, time_ = self._collect()
            except IndexError:
                self.__logger.error("Error with leaf payload")
                raise
            except ValueError:
                self.__logger.error("Error with batch schema")
                raise
            except IOError:
                self.__logger.error("IOError in writer")
                raise
            except Exception:
                self.__logger.error("Unknown error")
                raise
            self.__logger.debug("Allocated %i", pa.total_allocated_bytes())

    @timethis
    def _collect(self):
        """
        Collect all batches from the leaves
        Occurs after single input source is chunked
        Each chunked converted to a batch
        Batches on leaves collected
        Input file -> Output Arrow RecordBatches
        """
        self.__logger.debug(
            "artemis: collect: pyarrow malloc %i", pa.total_allocated_bytes()
        )

        self.__logger.debug("Leaves %s", self.gate.tree.leaves)
        for leaf in self.gate.tree.leaves:
            self.__logger.debug("Leaf node %s", leaf)
            node = self.gate.tree.get_node_by_key(leaf)
            els = node.payload
            self.__logger.debug("Batches of leaf %s", len(els))
            _name = "writer_" + node.key
            _last = None
            try:
                _last = els[-1].get_data()
            except IndexError:
                self.__logger.error("%s payload empty", leaf)
                raise
            except Exception:
                self.__logger.error("%s unknown error", leaf)
                raise
            if isinstance(_last, pa.lib.RecordBatch):
                self.__logger.debug("RecordBatch")
                self.__logger.debug("Allocated %i", pa.total_allocated_bytes())
                _schema_batch = els[-1].get_data().schema

                if self.gate.tools.get(_name)._schema != _schema_batch:
                    self.__logger.error("Schema mismatch")
                    self.__logger.error(
                        "Writer schema %s", self.gate.tools.get(_name)._schema
                    )
                    self.__logger.error("Batch schema %s", _schema_batch)
                    raise ValueError
                try:
                    self.gate.tools.get("writer_" + node.key).write(els)
                    self.__logger.debug(
                        "Records %i Batches %i",
                        self.gate.tools.get(_name)._nrecords,
                        self.gate.tools.get(_name)._nbatches,
                    )
                except IOError:
                    self.__logger.error("IOError in buffer writer")
                    raise
                except Exception:
                    self.__logger.error("Unknown error in buffer writer")
                    raise
            else:
                self.__logger.debug("%s", type(els[-1].get_data()))

        # Batches serialized, clear the tree to flush memory
        try:
            self.gate.tree.flush()
        except Exception:
            self.__logger("Problem flushing")
            raise

        self.__logger.debug("Allocated after write %i", pa.total_allocated_bytes())
        return True

    def finalize(self):
        """
        Ensure the data store is empty
        Spill any remaining arrow buffers to disk
        """
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
        summary = self.gate.meta.summary
        _wnames = []
        for leaf in self.gate.tree.leaves:
            self.__logger.debug("Leave node %s", leaf)
            node = self.gate.tree.get_node_by_key(leaf)
            key = node.key
            _wnames.append("writer_" + node.key)

        for key in _wnames:
            try:
                writer = self.gate.tools.get(key)
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
            self.__logger.info(
                "%s Records: %i Batches: %i Files: %i",
                writer.name,
                writer.total_records,
                writer.total_batches,
                writer.total_files,
            )

    def _flush_buffer(self):
        _wnames = []
        for leaf in Tree().leaves:
            self.__logger.debug("Leave node %s", leaf)
            node = Tree().get_node_by_key(leaf)
            key = node.key
            _wnames.append("writer_" + node.key)

        for key in _wnames:
            try:
                writer = self.gate.tools.get(key)
            except KeyError:
                continue
            try:
                writer.flush()
            except Exception:
                self.__logger.error("Flush buffer stream fails %s", key)
                raise
