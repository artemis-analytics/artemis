#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""
Property classes
"""

from collections import OrderedDict
from pprint import pformat

from artemis.logger import Logger
from artemis.core.singleton import Singleton
from artemis.io.protobuf.configuration_pb2 import Properties as Properties_pb
from artemis.io.protobuf.configuration_pb2 import Configuration
from artemis.io.protobuf.menu_pb2 import Menu
from artemis.io.protobuf.artemis_pb2 import JobInfo as JobInfo_pb
from artemis.utils.utils import bytes_to_mb
from artemis.core.book import ArtemisBook, TDigestBook
from artemis.meta.cronus import BaseObjectStore
from artemis.io.protobuf.cronus_pb2 import HistsObjectInfo, TDigestObjectInfo, JobObjectInfo


class Properties():
    '''
    Dynamically create getter/setter for user-defined properties
    '''
    def __init__(self, lock=False):
        self.lock = lock
        self.properties = dict()

    def __str__(self):
        return pformat(self.properties)

    def add_property(self, name, value):
        # Retain dictionary of properties
        self.properties[name] = value
        # Local fget and fset functions
        # lambdas defined directly in property below
        # fixes flake8 errors

        # add the property to self
        setattr(self.__class__, name,
                property(lambda self: self._get_property(name),
                         lambda self, value: self._set_property(name, value)))
        # add corresponding local variable
        setattr(self, '_' + name, value)

    @staticmethod
    def from_msg(msg):
        '''
        returns dict
        '''
        _supported = {'str': str,
                      'float': float,
                      'int': int,
                      'bool': bool,
                      'dict': dict}
        properties = {}
        for p in msg.property:
            if p.type == 'NoneType':
                continue
            elif p.type == 'dict' or p.type == 'bool' or p.type == 'list':
                properties[p.name] = eval(p.value)
            else:
                properties[p.name] = _supported[p.type](p.value)
        return properties

    def to_dict(self):
        '''
        Ordered dictionary of all user-defined properties
        '''
        _dict = OrderedDict()
        for key in self.properties:
            _dict[key] = self.properties[key]
        return _dict

    def to_msg(self):
        message = Properties_pb()
        for key in self.properties:
            if self.properties[key] is None:
                continue
            pbuf = message.property.add()
            pbuf.name = key
            pbuf.type = type(self.properties[key]).__name__
            pbuf.value = str(self.properties[key])
        return message

    def _set_property(self, name, value):
        if not self.lock:
            setattr(self, '_' + name, value)
        else:
            print('Cannot change "{}": properties are locked'
                  .format(name))

    def _get_property(self, name):
        return getattr(self, '_' + name)


@Logger.logged
class JobProperties(metaclass=Singleton):
    '''
    Wrapper class as Singleton type
    Holds JobProperties for use throughout framework
    '''
    def __init__(self):
        self.meta = JobInfo_pb()
        self.hbook = ArtemisBook()
        self.tbook = TDigestBook()
        self.menu = Menu()
        self.config = Configuration()
        self.store = None
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
    pass


class MetaMixin():
    '''
    Methods for setting / getting job attributes
    '''
    pass
