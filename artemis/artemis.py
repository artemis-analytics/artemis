#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""
Class for managing and running the job
owns the stores needed by the user algorithms
    histograms
    timers
    objects
"""
# Python libraries
import os
import traceback

# Externals
import pyarrow as pa

# Framework
from artemis.logger import Logger
# from artemis.exceptions import NullDataError

# Core
from artemis.core.properties import Properties
from artemis.core.properties import JobProperties
from artemis.core.steering import Steering
from artemis.core.tree import Tree
from artemis.core.algo import AlgoBase
from artemis.core.tool import ToolStore

# IO
from artemis.io.collector import Collector

# Protobuf
import artemis.io.protobuf.artemis_pb2 as artemis_pb2

# Utils
from artemis.utils.utils import bytes_to_mb, range_positive
from artemis.decorators import timethis


class ArtemisFactory:
    '''
    Factory class
    Update the configuration message from JobInfo message
    Allows for configuration to be static or predefined
    Reused for new datasets with different input and output data repos

    Requires updating
    bufferwriter tool with output repo path
    file generator tool with input repo path
    also replace a data generator with a file generator
    '''
    def __new__(cls, jobinfo, loglevel='INFO'):
        dirpath = jobinfo.output.repo

        if jobinfo.HasField('input'):
            if jobinfo.input.HasField('atom'):
                inpath = jobinfo.input.atom.repo
                glob = jobinfo.input.atom.glob
                generator = jobinfo.config.input.generator
                if generator.config.klass == 'FileGenerator':
                    for p in generator.config.properties.property:
                        if p.name == 'path':
                            p.value = inpath
                        if p.name == 'glob':
                            p.value = glob

        for tool in jobinfo.config.tools:
            if tool.name == 'bufferwriter':
                for p in tool.properties.property:
                    if p.name == 'path':
                        p.value = dirpath

        return Artemis(jobinfo, loglevel=loglevel)


@Logger.logged
class Artemis():

    def __init__(self, jobinfo, **kwargs):
        self.properties = Properties()
        self._jp = JobProperties()
        self._jp.configure(jobinfo)
        self._jp.job_state = artemis_pb2.JOB_STARTING
        # Logging
        logobj = self._jp.store.register_log(self._jp.meta.dataset_id,
                                             self._jp.meta.job_id)
        Logger.configure(self,
                         path=logobj.address,
                         loglevel=kwargs['loglevel'])
        #######################################################################

        # Define the internal objects for Artemis
        self.steer = None  # Manages traversing compute graph
        self.datahandler = None  # Manages input data
        self.filehandler = None  # Manages datum processing
        self.collector = None  # Manages Arrow malloc and serialization
        # List of timer histos for easy access
        self.__tools = ToolStore()

    def control(self):
        '''
        Stateful Job processing via pytransitions
        '''
        self._jp.job_state = artemis_pb2.JOB_RUNNING
        self.launch()

        # Configure Artemis job
        self._jp.job_state = artemis_pb2.JOB_CONFIGURE
        try:
            self.configure()
        except Exception as e:
            self.logger.error('Caught error in configure')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        try:
            self.lock()
        except Exception as e:
            self.logger.error('Caught error in lock')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        self._jp.job_state = artemis_pb2.JOB_INITIALIZE
        try:
            self.initialize()
        except Exception as e:
            self.logger.error('Caught error in initialize')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        # Book
        # Histograms
        # Timers
        self._jp.job_state = artemis_pb2.JOB_BOOK
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
        self._jp.job_state = artemis_pb2.JOB_SAMPLE
        try:
            r, time_ = self.execute()
            self._jp.hbook.fill('artemis', 'time.execute', time_)
        except Exception as e:
            self.logger.error('Caught error in sample_chunks')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        self._jp.job_state = artemis_pb2.JOB_REBOOK
        try:
            self.rebook()
        except Exception as e:
            self.logger.error('Caught error in rebook')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        try:
            self.collector.initialize()
        except Exception as e:
            self.logger.error('Caught error in init_buffers')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        # Clear all memory and raw data
        try:
            Tree().flush()
            self.datum = None
        except Exception as e:
            self.logger.error('Caught error in Tree.flush')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        self.__logger.info("artemis: sampleing complete malloc %i",
                           pa.total_allocated_bytes())
        self._jp.job_state = artemis_pb2.JOB_EXECUTE
        try:
            r, time_ = self.execute()
            self._jp.hbook.fill('artemis', 'time.execute', time_)
        except Exception as e:
            self.logger.error("Unexcepted error caught in run")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        self._jp.job_state = artemis_pb2.JOB_FINALIZE
        try:
            self.finalize()
        except Exception as e:
            self.logger.error("Unexcepted error caught in finalize")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

    def _set_path(self, **kwargs):
        if hasattr(self.properties, 'path'):
            if os.path.exists(self.properties.path) is False:
                raise IOError
            self._path = os.path.abspath(self.properties.path)
        else:
            self._path = ''

    def _set_job_id(self):
        '''
        Creates unique basename for output data
        '''
        _bname = self._jp.meta.name
        _id = self._jp.meta.job_id
        self._job_id = _bname + '-' + _id

    def _update_state(self, state):
        self._jp.meta.state = state

    def launch(self):
        self.logger.info('Artemis is ready')

    def configure(self):
        '''
        Configure global job dependencies
        such as DB connections
        Create the histogram store
        '''
        self.__logger.info('Configure')
        self.__logger.info("%s properties: %s",
                           self.__class__.__name__,
                           self.properties)
        self.__logger.info("Job ID %s", self._jp.job_id)

        # Create Steering instance
        self.__logger.info(self._jp.config)
        self.steer = Steering('steer', loglevel=Logger.CONFIGURED_LEVEL)

        # Create the collector
        # Monitors the arrow memory pool
        self.collector = \
            Collector('collector',
                      max_malloc=self._jp.config.max_malloc_size_bytes,
                      job_id=self._jp.job_id,
                      loglevel=Logger.CONFIGURED_LEVEL)

        # Configure the data handler
        _msggen = self._jp.config.input.generator.config
        try:
            self.datahandler = AlgoBase.from_msg(self.__logger, _msggen)
        except Exception:
            self.__logger.info("Failed to load generator from protomsg")
            raise

        # Add tools
        for toolcfg in self._jp.config.tools:
            self.__logger.info("Add Tool %s", toolcfg.name)
            self.__tools.add(self.__logger, toolcfg)

    def lock(self):
        '''
        Lock all properties before initialize
        '''
        # TODO
        # Exceptions?
        self.__logger.info("{}: Lock".format('artemis'))
        self.properties.lock = True
        try:
            self.steer.lock()
        except Exception:
            self.__logger("cannot lock steering")
            raise

    def initialize(self):
        self.__logger.info("{}: Initialize".format('artemis'))

        try:
            self.steer.initialize()
        except Exception:
            self.__logger.error('Cannot initialize Steering')
            raise

        try:
            self.datahandler.initialize()
        except Exception:
            self.__logger.error("Cannot initialize algo %s" % 'generator')
            raise

        for toolcfg in self._jp.config.tools:
            if toolcfg.name == "bufferwriter":
                continue
            try:
                self.__tools.get(toolcfg.name).initialize()
            except Exception:
                self.__logger.error("Cannot initialize %s", toolcfg.name)

        self.filehandler = self.__tools.get("filehandler")

    def book(self):
        self.__logger.info("Book")
        self._jp.hbook.book('artemis', 'counts', range(10))
        bins = [x for x in range_positive(0., 10., 0.1)]

        # Payload and block distributions
        self._jp.hbook.book('artemis', 'payload', bins, 'MB', timer=True)
        self._jp.hbook.book('artemis', 'nblocks', range(100), 'n', timer=True)
        self._jp.hbook.book('artemis', 'blocksize', bins, 'MB', timer=True)

        # Timing plots
        bins = [x for x in range_positive(0., 1000., 2.)]
        self._jp.hbook.book('artemis', 'time.prepblks',
                            bins, 'ms', timer=True)
        self._jp.hbook.book('artemis', 'time.prepschema',
                            bins, 'ms', timer=True)
        self._jp.hbook.book('artemis', 'time.execute',
                            bins, 'ms', timer=True)
        self._jp.hbook.book('artemis', 'time.collect',
                            bins, 'ms', timer=True)
        self._jp.hbook.book('artemis', 'time.steer',
                            bins, 'ms', timer=True)

        try:
            self.steer.book()
        except Exception:
            self.__logger.error('Cannot book Steering')
            raise

    def rebook(self):
        '''
        Rebook histograms for timers or profiles
        after random sampling of data chunk
        '''
        self.__logger.info("Rebook")

        self._jp.hbook.rebook()  # Resets all histograms!

        self.__logger.info("artemis: allocated before reset %i",
                           pa.total_allocated_bytes())
        self.datahandler.reset()
        self.__logger.info("artemis: allocated after reset %i",
                           pa.total_allocated_bytes())

        # Reset all meta data needed for processing all job info
        _summary = self._jp.meta.summary
        _summary.processed_bytes = 0
        _summary.processed_ndatums = 0

    @timethis
    def execute(self):
        '''
        Event Loop
        Prepare an input datum
        Process each chunk, passing to Steering
        After all chunks processed, collect to output buffers
        Clear all data, move to next datum

        '''
        self.__logger.info("Execute")
        self.__logger.debug('artemis: Count at run call %i',
                            self._jp.meta.summary.processed_ndatums)

        self.__logger.info("artemis: Run: pyarrow malloc %i",
                           pa.total_allocated_bytes())

        if self._jp.meta.state == artemis_pb2.JOB_SAMPLE:
            self.__logger.info("Iterate over samples")
            iter_datum = self.datahandler.sampler()
        elif self._jp.meta.state == artemis_pb2.JOB_EXECUTE:
            self.__logger.info("Iterate over Datums")
            iter_datum = self.datahandler
        else:
            self.__logger.error("Unknown job state for execute")
            raise ValueError

        for datum in iter_datum:
            if isinstance(datum, bytes):
                datum = pa.py_buffer(datum)

            self._jp.hbook.fill('artemis', 'counts',
                                self._jp.meta.summary.processed_ndatums)
            try:
                reader = self.filehandler.execute(datum)
            except Exception:
                self.__logger.error("Failed to prepare file")
                raise

            self._jp.hbook.fill('artemis', 'payload',
                                bytes_to_mb(self.filehandler.size_bytes))
            self._jp.hbook.fill('artemis', 'nblocks',
                                len(self.filehandler.blocks))

            self.__logger.info("artemis: flush before execute %i",
                               pa.total_allocated_bytes())

            if self._jp.meta.state == artemis_pb2.JOB_SAMPLE:
                self.__logger.info("Iterate over samples")
                iter_batches = reader.sampler()
            elif self._jp.meta.state == artemis_pb2.JOB_EXECUTE:
                iter_batches = reader
            else:
                self.__logger.error("Unknown job state for execute")
                raise ValueError

            for batch in iter_batches:
                self._jp.hbook.fill('artemis', 'blocksize',
                                    bytes_to_mb(batch.size))
                steer_exec = timethis(self.steer.execute)
                try:
                    r, time_ = steer_exec(batch)
                except Exception:
                    self.__logger.error("Problem executing sample")
                    raise
                self.__logger.debug("artemis: execute complete malloc %i",
                                    pa.total_allocated_bytes())
                self._jp.hbook.fill('artemis', 'time.steer', time_)
                self._jp.meta.summary.processed_bytes += batch.size

                if self._jp.meta.state == artemis_pb2.JOB_EXECUTE:
                    try:
                        self.collector.execute()
                    except Exception:
                        self.__logger.error("Fail to collect")
                        raise
            self.__logger.info('Processed %i' %
                               self._jp.meta.summary.processed_bytes)

    def _finalize_jobstate(self, state):
        self._update_state(state)
        self._jp.meta.finished.GetCurrentTime()
        duration = self._jp.meta.summary.job_time
        duration.seconds = self._jp.meta.finished.seconds -\
            self._jp.meta.started.seconds
        duration.nanos = self._jp.meta.finished.nanos -\
            self._jp.meta.started.nanos
        if duration.seconds < 0 and duration.nanos > 0:
            duration.seconds += 1
            duration.nanos -= 1000000000
        elif duration.seconds > 0 and duration.nanos < 0:
            duration.seconds -= 1
            duration.nanos += 1000000000

    def finalize(self):
        self.__logger.info("Finalizing Artemis job %s" %
                           self._jp.meta.name)
        summary = self._jp.meta.summary

        try:
            self.steer.finalize()
        except Exception:
            self.__logger.error("Steer finalize fails")
            raise

        mu_payload = self._jp.hbook['artemis.payload'].mean()
        mu_blocksize = self._jp.hbook['artemis.blocksize'].mean()
        mu_nblocks = self._jp.hbook['artemis.nblocks'].mean()

        for key in self._jp.hbook.keys():
            if 'time' not in key:
                continue
            mu = self._jp.hbook[key].mean()
            std = self._jp.hbook[key].std()

            # Add to the msg
            msgtime = summary.timers.add()
            msgtime.name = key
            msgtime.time = mu
            msgtime.std = std

        try:
            self.collector.finalize()
        except Exception:
            self.__logger.error("Collector fails to finalize buffer")
            raise

        self._finalize_jobstate(artemis_pb2.JOB_SUCCESS)

        try:
            self._jp.finalize()
        except IOError:
            self.__logger.error("Cannot write proto")
        except Exception:
            raise

        self.__logger.info("Processed data summary")
        self.__logger.info("Mean payload %2.2f MB", mu_payload)
        self.__logger.info("Mean blocksize %2.2f MB", mu_blocksize)
        self.__logger.info("Mean n block %2.2f ", mu_nblocks)

    def abort(self, *args, **kwargs):
        self._jp.meta.state = artemis_pb2.JOB_ABORT
        self.__logger.error("Artemis has been triggered to Abort")
        self.__logger.error("Reason %s" % args[0])

        try:
            self.collector.finalize()
        except Exception:
            self.__logger.error("Collector fails to finalize buffer")
            raise

        self._jp.meta.finished.GetCurrentTime()
        jobinfoname = self._job_id + '_meta.dat'
        try:
            with open(jobinfoname, "wb") as f:
                f.write(self._jp.meta.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write hbook")
        except Exception as e:
            error_message = traceback.format_exc()
            self.__logger.error("Meta data write fails. Reason: %s", e)
            self.__logger.error(error_message)
