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
Artemis Core Application
"""

# Python libraries
import traceback

# Externals
import pyarrow as pa

# Framework
from artemis.logger import Logger

# from artemis.exceptions import NullDataError

# Core
from artemis.core.properties import Properties
from artemis.core.gate import ArtemisGateSvc
from artemis.core.gate import MetaMixin, IOMetaMixin
from artemis.core.steering import Steering
from artemis.core.algo import AlgoBase

# IO
from artemis.io.collector import Collector

# Protobuf
import artemis.io.protobuf.artemis_pb2 as artemis_pb2

# Utils
from artemis.utils.utils import bytes_to_mb, range_positive
from artemis.decorators import timethis


class ArtemisFactory:
    """
    Deprecated Factory class
    Update the configuration message from JobInfo message
    Allows for configuration to be static or predefined
    Reused for new datasets with different input and output data repos

    Requires updating
    bufferwriter tool with output repo path
    file generator tool with input repo path
    also replace a data generator with a file generator
    """

    def __new__(cls, jobinfo, loglevel="INFO"):
        dirpath = jobinfo.output.repo

        if jobinfo.HasField("input"):
            if jobinfo.input.HasField("atom"):
                inpath = jobinfo.input.atom.repo
                glob = jobinfo.input.atom.glob
                generator = jobinfo.config.input.generator
                if generator.config.klass == "FileGenerator":
                    for p in generator.config.properties.property:
                        if p.name == "path":
                            p.value = inpath
                        if p.name == "glob":
                            p.value = glob

        for tool in jobinfo.config.tools:
            if tool.name == "bufferwriter":
                for p in tool.properties.property:
                    if p.name == "path":
                        p.value = dirpath

        return Artemis(jobinfo, loglevel=loglevel)


@Logger.logged
class Artemis(MetaMixin, IOMetaMixin):
    """Top-level Artemis framework class.
    Manages event loop, error handling and control flow
    Mixin classes provide methods for managing metadata

    Attributes
    ----------
        properties : Properties
            Stored metadata properties for Artemis
        gate : ArtemisGatframework level data, timers, histograms, metadata
        job_state : enum
            Enumerated job state
        steer : Steering
            steering instance to manage execution of process graph
        datahandler : generator
            generator for serving data to artemis
        filehandler : FileHandler
            file handler for managing processing of datums
        collector : Collector
            collector class, monitors Arrow memory pool and writers to spill to disk

    Parameters
    ----------
        jobinfo : JobInfo_pb
            JobInfo Protocol buffer

    Other Parameters
    ----------------
        loglevel : str
            Optional level for logging `INFO`, `DEBUG`, `VERBOSE`

    Returns
    -------

    Examples
    --------
    """

    def __init__(self, jobinfo, **kwargs):
        self.properties = Properties()
        self.gate = ArtemisGateSvc()
        self.gate.configure(jobinfo)
        self.job_state = artemis_pb2.JOB_STARTING
        # Logging
        logobj = self.register_log()
        Logger.configure(self, path=logobj.address, loglevel=kwargs["loglevel"])
        #######################################################################

        # Define the internal objects for Artemis
        self.steer = None  # Manages traversing compute graph
        self.datahandler = None  # Manages input data
        self.filehandler = None  # Manages datum processing
        self.collector = None  # Manages Arrow malloc and serialization

    def control(self):
        """
        Execute an artemis sub-job

        Parameters
        ----------

        Other Parameters
        ----------------

        Returns
        -------
            True : bool
            The return code :: False -- exception encountered
            sends error to :class:`artemis.Artemis.abort`
        """
        self.job_state = artemis_pb2.JOB_RUNNING
        self.launch()

        # Configure Artemis job
        self.job_state = artemis_pb2.JOB_CONFIGURE
        try:
            self.configure()
        except Exception as e:
            self.logger.error("Caught error in configure")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        try:
            self.lock()
        except Exception as e:
            self.logger.error("Caught error in lock")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        self.job_state = artemis_pb2.JOB_INITIALIZE
        try:
            self.initialize()
        except Exception as e:
            self.logger.error("Caught error in initialize")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        # Book
        # Histograms
        # Timers
        self.job_state = artemis_pb2.JOB_BOOK
        try:
            self.book()
        except Exception as e:
            self.logger.error("Cannot book")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        # In order to set the output buffer schema
        # we need to get a schema first
        # In order to set the timing histograms, we need to profile
        # sample first
        # Artemis should always run in a sampling mode first
        # Make number of samples configurable
        self.job_state = artemis_pb2.JOB_SAMPLE
        try:
            r, time_ = self.execute()
            self.gate.hbook.fill("artemis", "time.execute", time_)
        except Exception as e:
            self.logger.error("Caught error in sample_chunks")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        self.job_state = artemis_pb2.JOB_REBOOK
        try:
            self.rebook()
        except Exception as e:
            self.logger.error("Caught error in rebook")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        try:
            self.collector.initialize()
        except Exception as e:
            self.logger.error("Caught error in init_buffers")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        # Clear all memory and raw data
        try:
            self.gate.tree.flush()
            self.datum = None
        except Exception as e:
            self.logger.error("Caught error in Tree.flush")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        self.__logger.info(
            "artemis: sampleing complete malloc %i", pa.total_allocated_bytes()
        )
        self.job_state = artemis_pb2.JOB_EXECUTE
        try:
            r, time_ = self.execute()
            self.gate.hbook.fill("artemis", "time.execute", time_)
        except Exception as e:
            self.logger.error("Unexcepted error caught in run")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        self.job_state = artemis_pb2.JOB_FINALIZE
        try:
            self.finalize()
        except Exception as e:
            self.logger.error("Unexcepted error caught in finalize")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

    def launch(self):
        """
        This function announces that Artemis sub-job is beginning
        """
        self.logger.info("Artemis is ready")

    def configure(self):
        """Configures all sub-job dependencies.

        instantiate :class:`artemis.core.steering.Steering`.

        instantiate :class:`artemis.io.collector.Collector`.

        instantiate a datahandler.

        retrieve tool metadata

        add tools to metastore
        """
        self.__logger.info("Configure")
        self.__logger.info(
            "%s properties: %s", self.__class__.__name__, self.properties
        )
        self.__logger.info("Job ID %s", self.job_id)

        # Create Steering instance
        self.__logger.info(self.gate.config)
        self.steer = Steering("steer", loglevel=Logger.CONFIGURED_LEVEL)

        # Create the collector
        # Monitors the arrow memory pool
        self.collector = Collector(
            "collector",
            max_malloc=self.gate.config.max_malloc_size_bytes,
            job_id=self.job_id,
            loglevel=Logger.CONFIGURED_LEVEL,
        )

        # Configure the data handler
        _msggen = self.gate.config.input.generator.config
        try:
            self.datahandler = AlgoBase.from_msg(self.__logger, _msggen)
        except Exception:
            self.__logger.info("Failed to load generator from protomsg")
            raise

        # Add tools
        for name in self.gate.config.tools:
            toolcfg = self.gate.config.tools[name]
            self.__logger.info("Add Tool %s", toolcfg.name)
            self.gate.tools.add(self.__logger, toolcfg)

    def lock(self):
        """Lock all properties before initialize
        """
        # TODO
        # Exceptions?
        self.__logger.info("{}: Lock".format("artemis"))
        self.properties.lock = True
        try:
            self.steer.lock()
        except Exception:
            self.__logger("cannot lock steering")
            raise

    def initialize(self):
        """Initialize all algorithms and tools
        """
        self.__logger.info("{}: Initialize".format("artemis"))

        try:
            self.steer.initialize()
        except Exception:
            self.__logger.error("Cannot initialize Steering")
            raise

        try:
            self.datahandler.initialize()
        except Exception:
            self.__logger.error("Cannot initialize algo %s" % "generator")
            raise

        for name in self.gate.tools.keys():
            if name == "bufferwriter":
                continue
            try:
                self.get_tool(name).initialize()
            except Exception:
                self.__logger.error("Cannot initialize %s", name)
                raise

        self.filehandler = self.gate.tools.get("filehandler")

    def book(self):
        """
        book all algorithm histograms via call to steering.book
        """
        self.__logger.info("Book")
        self.gate.hbook.book("artemis", "counts", range(10))
        bins = [x for x in range_positive(0.0, 10.0, 0.1)]

        # Payload and block distributions
        self.gate.hbook.book("artemis", "payload", bins, "MB", timer=True)
        self.gate.hbook.book("artemis", "nblocks", range(100), "n", timer=True)
        self.gate.hbook.book("artemis", "blocksize", bins, "MB", timer=True)

        # Timing plots
        bins = [x for x in range_positive(0.0, 1000.0, 2.0)]
        self.gate.hbook.book("artemis", "time.prepblks", bins, "ms", timer=True)
        self.gate.hbook.book("artemis", "time.prepschema", bins, "ms", timer=True)
        self.gate.hbook.book("artemis", "time.execute", bins, "ms", timer=True)
        self.gate.hbook.book("artemis", "time.collect", bins, "ms", timer=True)
        self.gate.hbook.book("artemis", "time.steer", bins, "ms", timer=True)

        try:
            self.steer.book()
        except Exception:
            self.__logger.error("Cannot book Steering")
            raise

    def rebook(self):
        """Rebook histograms for timers or profiles.
        typically called after random sampling of data chunk
        """
        self.__logger.info("Rebook")

        self.gate.hbook.rebook()  # Resets all histograms!

        self.__logger.info(
            "artemis: allocated before reset %i", pa.total_allocated_bytes()
        )
        self.datahandler.reset()
        self.__logger.info(
            "artemis: allocated after reset %i", pa.total_allocated_bytes()
        )

        # Reset all meta data needed for processing all job info
        self.reset_job_summary()

    @timethis
    def execute(self):
        """Event Loop execution
        Prepare an input datum, e.g. a file
        Process each chunk, passing to Steering
        After all chunks processed, collect to output buffers
        Clear all data, move to next datum
        """
        self.__logger.info("Execute")
        self.__logger.debug(
            "artemis: Count at run call %i", self.gate.meta.summary.processed_ndatums
        )

        self.__logger.info(
            "artemis: Run: pyarrow malloc %i", pa.total_allocated_bytes()
        )

        if self.job_state == artemis_pb2.JOB_SAMPLE:
            self.__logger.info("Iterate over samples")
            iter_datum = self.datahandler.sampler()
        elif self.job_state == artemis_pb2.JOB_EXECUTE:
            self.__logger.info("Iterate over Datums")
            iter_datum = self.datahandler
        else:
            self.__logger.error("Unknown job state for execute")
            raise ValueError

        for datum in iter_datum:
            if isinstance(datum, bytes):
                datum = pa.py_buffer(datum)

            self.gate.hbook.fill(
                "artemis", "counts", self.gate.meta.summary.processed_ndatums
            )
            try:
                reader = self.filehandler.execute(datum)
            except Exception:
                self.__logger.error("Failed to prepare file")
                raise

            self.gate.hbook.fill(
                "artemis", "payload", bytes_to_mb(self.filehandler.size_bytes)
            )
            self.gate.hbook.fill("artemis", "nblocks", len(self.filehandler.blocks))

            self.__logger.info(
                "artemis: flush before execute %i", pa.total_allocated_bytes()
            )

            if self.job_state == artemis_pb2.JOB_SAMPLE:
                self.__logger.info("Iterate over samples")
                iter_batches = reader.sampler()
            elif self.job_state == artemis_pb2.JOB_EXECUTE:
                iter_batches = reader
            else:
                self.__logger.error("Unknown job state for execute %s", self.job_state)
                raise ValueError

            for batch in iter_batches:
                self.gate.hbook.fill("artemis", "blocksize", bytes_to_mb(batch.size))
                steer_exec = timethis(self.steer.execute)
                try:
                    r, time_ = steer_exec(batch)
                except Exception:
                    self.__logger.error("Problem executing sample")
                    raise
                self.__logger.debug(
                    "artemis: execute complete malloc %i", pa.total_allocated_bytes()
                )
                self.gate.hbook.fill("artemis", "time.steer", time_)
                self.processed_bytes = batch.size

                if self.job_state == artemis_pb2.JOB_EXECUTE:
                    try:
                        self.collector.execute()
                    except Exception:
                        self.__logger.error("Fail to collect")
                        raise

            # Update datum input count
            self.processed_ndatums = 1

            self.__logger.info("Processed %i" % self.gate.meta.summary.processed_bytes)

    def finalize(self):
        """finalize Artemis sub-job.

        call :meth:`artemis.core.steering.Steering.finalize`.
        call :meth:`artemis.io.collector.Collector.finalize`.
        call :meth:`artemis.core.gate.ArtemisGateSvc.finalize`.

        Parameters
        ----------

        Returns
        -------

        Raises
        ------
            Exception
                unknown errors that are not caught are raised as Exception
            IOError
                if `gate` cannot write
        """
        self.__logger.info("Finalizing Artemis job %s" % self.gate.meta.name)

        try:
            self.steer.finalize()
        except Exception:
            self.__logger.error("Steer finalize fails")
            raise

        try:
            self.collector.finalize()
        except Exception:
            self.__logger.error("Collector fails to finalize buffer")
            raise

        try:
            self.gate.finalize()
        except IOError:
            self.__logger.error("Cannot write proto")
        except Exception:
            raise

        self.__logger.info("Job Complete with state %s", self.job_state)

    def abort(self, *args, **kwargs):
        """abort Artemis sub-job
        Unknown Exceptions or Exceptions which require aborting job are propagated.

        :meth:`artemis.io.collector.Collector.finalize`.
        :meth:`artemis.core.gate.ArtemisGateSvc.finalize`.

        Parameters
        ----------
            Exception

        Other Parameters
        ----------------
            Exception

        Raises
        ------

        """
        self.state = artemis_pb2.JOB_ABORT
        self.__logger.error("Artemis has been triggered to Abort")
        self.__logger.error("Reason %s" % args[0])

        try:
            self.collector.finalize()
        except Exception:
            self.__logger.error("Collector fails to finalize buffer")
            raise

        self.gate.meta.finished.GetCurrentTime()
        try:
            self.gate.finalize()
        except IOError:
            self.__logger.error("Cannot finalize job output")
        except Exception as e:
            error_message = traceback.format_exc()
            self.__logger.error("Meta data write fails. Reason: %s", e)
            self.__logger.error(error_message)
