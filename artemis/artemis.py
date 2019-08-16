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
from artemis.core.gate import ArtemisGateSvc
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
        self.gate = ArtemisGateSvc()
        self.gate.configure(jobinfo)
        self.gate.job_state = artemis_pb2.JOB_STARTING
        # Logging
        logobj = self.gate.store.register_log(self.gate.meta.dataset_id,
                                              self.gate.meta.job_id)
        Logger.configure(self,
                         path=logobj.address,
                         loglevel=kwargs['loglevel'])
        #######################################################################

        # Define the internal objects for Artemis
        self.steer = None  # Manages traversing compute graph
        self.datahandler = None  # Manages input data
        self.filehandler = None  # Manages datum processing
        self.collector = None  # Manages Arrow malloc and serialization

    def control(self):
        '''
        Stateful Job processing via pytransitions
        '''
        self.gate.job_state = artemis_pb2.JOB_RUNNING
        self.launch()

        # Configure Artemis job
        self.gate.job_state = artemis_pb2.JOB_CONFIGURE
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

        self.gate.job_state = artemis_pb2.JOB_INITIALIZE
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
        self.gate.job_state = artemis_pb2.JOB_BOOK
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
        self.gate.job_state = artemis_pb2.JOB_SAMPLE
        try:
            r, time_ = self.execute()
            self.gate.hbook.fill('artemis', 'time.execute', time_)
        except Exception as e:
            self.logger.error('Caught error in sample_chunks')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        self.gate.job_state = artemis_pb2.JOB_REBOOK
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
            self.gate.tree.flush()
            self.datum = None
        except Exception as e:
            self.logger.error('Caught error in Tree.flush')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        self.__logger.info("artemis: sampleing complete malloc %i",
                           pa.total_allocated_bytes())
        self.gate.job_state = artemis_pb2.JOB_EXECUTE
        try:
            r, time_ = self.execute()
            self.gate.hbook.fill('artemis', 'time.execute', time_)
        except Exception as e:
            self.logger.error("Unexcepted error caught in run")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        self.gate.job_state = artemis_pb2.JOB_FINALIZE
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
        _bname = self.gate.meta.name
        _id = self.gate.meta.job_id
        self._job_id = _bname + '-' + _id

    def _update_state(self, state):
        self.gate.meta.state = state

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
        self.__logger.info("Job ID %s", self.gate.job_id)

        # Create Steering instance
        self.__logger.info(self.gate.config)
        self.steer = Steering('steer', loglevel=Logger.CONFIGURED_LEVEL)

        # Create the collector
        # Monitors the arrow memory pool
        self.collector = \
            Collector('collector',
                      max_malloc=self.gate.config.max_malloc_size_bytes,
                      job_id=self.gate.job_id,
                      loglevel=Logger.CONFIGURED_LEVEL)

        # Configure the data handler
        _msggen = self.gate.config.input.generator.config
        try:
            self.datahandler = AlgoBase.from_msg(self.__logger, _msggen)
        except Exception:
            self.__logger.info("Failed to load generator from protomsg")
            raise

        # Add tools
        for toolcfg in self.gate.config.tools:
            self.__logger.info("Add Tool %s", toolcfg.name)
            self.gate.tools.add(self.__logger, toolcfg)

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

        for toolcfg in self.gate.config.tools:
            if toolcfg.name == "bufferwriter":
                continue
            try:
                self.gate.tools.get(toolcfg.name).initialize()
            except Exception:
                self.__logger.error("Cannot initialize %s", toolcfg.name)

        self.filehandler = self.gate.tools.get("filehandler")

    def book(self):
        self.__logger.info("Book")
        self.gate.hbook.book('artemis', 'counts', range(10))
        bins = [x for x in range_positive(0., 10., 0.1)]

        # Payload and block distributions
        self.gate.hbook.book('artemis', 'payload', bins, 'MB', timer=True)
        self.gate.hbook.book('artemis', 'nblocks', range(100), 'n', timer=True)
        self.gate.hbook.book('artemis', 'blocksize', bins, 'MB', timer=True)

        # Timing plots
        bins = [x for x in range_positive(0., 1000., 2.)]
        self.gate.hbook.book('artemis', 'time.prepblks',
                             bins, 'ms', timer=True)
        self.gate.hbook.book('artemis', 'time.prepschema',
                             bins, 'ms', timer=True)
        self.gate.hbook.book('artemis', 'time.execute',
                             bins, 'ms', timer=True)
        self.gate.hbook.book('artemis', 'time.collect',
                             bins, 'ms', timer=True)
        self.gate.hbook.book('artemis', 'time.steer',
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

        self.gate.hbook.rebook()  # Resets all histograms!

        self.__logger.info("artemis: allocated before reset %i",
                           pa.total_allocated_bytes())
        self.datahandler.reset()
        self.__logger.info("artemis: allocated after reset %i",
                           pa.total_allocated_bytes())

        # Reset all meta data needed for processing all job info
        _summary = self.gate.meta.summary
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
                            self.gate.meta.summary.processed_ndatums)

        self.__logger.info("artemis: Run: pyarrow malloc %i",
                           pa.total_allocated_bytes())

        if self.gate.meta.state == artemis_pb2.JOB_SAMPLE:
            self.__logger.info("Iterate over samples")
            iter_datum = self.datahandler.sampler()
        elif self.gate.meta.state == artemis_pb2.JOB_EXECUTE:
            self.__logger.info("Iterate over Datums")
            iter_datum = self.datahandler
        else:
            self.__logger.error("Unknown job state for execute")
            raise ValueError

        for datum in iter_datum:
            if isinstance(datum, bytes):
                datum = pa.py_buffer(datum)

            self.gate.hbook.fill('artemis', 'counts',
                                 self.gate.meta.summary.processed_ndatums)
            try:
                reader = self.filehandler.execute(datum)
            except Exception:
                self.__logger.error("Failed to prepare file")
                raise

            self.gate.hbook.fill('artemis', 'payload',
                                 bytes_to_mb(self.filehandler.size_bytes))
            self.gate.hbook.fill('artemis', 'nblocks',
                                 len(self.filehandler.blocks))

            self.__logger.info("artemis: flush before execute %i",
                               pa.total_allocated_bytes())

            if self.gate.meta.state == artemis_pb2.JOB_SAMPLE:
                self.__logger.info("Iterate over samples")
                iter_batches = reader.sampler()
            elif self.gate.meta.state == artemis_pb2.JOB_EXECUTE:
                iter_batches = reader
            else:
                self.__logger.error("Unknown job state for execute")
                raise ValueError

            for batch in iter_batches:
                self.gate.hbook.fill('artemis', 'blocksize',
                                     bytes_to_mb(batch.size))
                steer_exec = timethis(self.steer.execute)
                try:
                    r, time_ = steer_exec(batch)
                except Exception:
                    self.__logger.error("Problem executing sample")
                    raise
                self.__logger.debug("artemis: execute complete malloc %i",
                                    pa.total_allocated_bytes())
                self.gate.hbook.fill('artemis', 'time.steer', time_)
                self.gate.meta.summary.processed_bytes += batch.size

                if self.gate.meta.state == artemis_pb2.JOB_EXECUTE:
                    try:
                        self.collector.execute()
                    except Exception:
                        self.__logger.error("Fail to collect")
                        raise
            self.__logger.info('Processed %i' %
                               self.gate.meta.summary.processed_bytes)

    def _finalize_jobstate(self, state):
        self._update_state(state)
        self.gate.meta.finished.GetCurrentTime()
        duration = self.gate.meta.summary.job_time
        duration.seconds = self.gate.meta.finished.seconds -\
            self.gate.meta.started.seconds
        duration.nanos = self.gate.meta.finished.nanos -\
            self.gate.meta.started.nanos
        if duration.seconds < 0 and duration.nanos > 0:
            duration.seconds += 1
            duration.nanos -= 1000000000
        elif duration.seconds > 0 and duration.nanos < 0:
            duration.seconds -= 1
            duration.nanos += 1000000000

    def finalize(self):
        self.__logger.info("Finalizing Artemis job %s" %
                           self.gate.meta.name)
        summary = self.gate.meta.summary

        try:
            self.steer.finalize()
        except Exception:
            self.__logger.error("Steer finalize fails")
            raise

        mu_payload = self.gate.hbook['artemis.payload'].mean()
        mu_blocksize = self.gate.hbook['artemis.blocksize'].mean()
        mu_nblocks = self.gate.hbook['artemis.nblocks'].mean()

        for key in self.gate.hbook.keys():
            if 'time' not in key:
                continue
            mu = self.gate.hbook[key].mean()
            std = self.gate.hbook[key].std()

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
            self.gate.finalize()
        except IOError:
            self.__logger.error("Cannot write proto")
        except Exception:
            raise

        self.__logger.info("Processed data summary")
        self.__logger.info("Mean payload %2.2f MB", mu_payload)
        self.__logger.info("Mean blocksize %2.2f MB", mu_blocksize)
        self.__logger.info("Mean n block %2.2f ", mu_nblocks)

    def abort(self, *args, **kwargs):
        self.gate.meta.state = artemis_pb2.JOB_ABORT
        self.__logger.error("Artemis has been triggered to Abort")
        self.__logger.error("Reason %s" % args[0])

        try:
            self.collector.finalize()
        except Exception:
            self.__logger.error("Collector fails to finalize buffer")
            raise

        self.gate.meta.finished.GetCurrentTime()
        jobinfoname = self._job_id + '_meta.dat'
        try:
            with open(jobinfoname, "wb") as f:
                f.write(self.gate.meta.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write hbook")
        except Exception as e:
            error_message = traceback.format_exc()
            self.__logger.error("Meta data write fails. Reason: %s", e)
            self.__logger.error(error_message)
