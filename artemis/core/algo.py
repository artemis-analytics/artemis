#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""
Algorithms
"""
import logging
from collections import OrderedDict
import importlib
from pprint import pformat

from artemis.logger import Logger
from artemis.core.properties import Properties, JobProperties
from artemis.io.protobuf.artemis_pb2 import Algo as Algo_pb

# TODO Create an interface class to AlgoBase to expose the run,
# finalize methods to framework
# Interface IAlgoBase class to expose the methods to the framework
# (apparently, I should not write a framework, see Fluent Python ...
# I am bored but probably getting paid)
# Concrete implementation of interface with AlgoBase
# Concrete base class provides the mixins or other ABCs
# Likely we want to provide the Job class instance to retrieve
# job.histbook
# job.timers
# job.objectstore
# Inherited classes for user-defined methods MyAlgo


class AbcAlgoBase(type):
    '''
    https://stackoverflow.com/questions/29069655/python-logging-with-a-common-logger-class-mixin-and-class-inheritance

    Logger for the Base class and each derived class.
    Not for instances though
    To identify logging from different configurations
    pass the instance name (attribute)
    '''
    def __init__(cls, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Explicit name mangling
        logger_attribute_name = '_' + cls.__name__ + '__logger'

        # Logger name derived accounting for inheritance for the bonus marks
        logger_name = '.'.join([c.__name__ for c in cls.mro()[-2::-1]])

        def fget(cls): return getattr(cls, logger_attribute_name)

        # add the getter property to cls
        setattr(cls, 'logger', property(fget))
        # add the logger to cls
        setattr(cls, logger_attribute_name, logging.getLogger(logger_name))


class AlgoBase(metaclass=AbcAlgoBase):

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
        # Configure logging
        Logger.configure(self, **kwargs)

        self.__logger.debug('__init__ AlgoBase')
        # name will be mangled to _AlgoBase__name
        self.__name = name
        self.properties = Properties()
        for key in kwargs:
            self.properties.add_property(key, kwargs[key])

        self._jp = JobProperties()

    def __init_subclass__(cls, **kwargs):
        '''
        See PEP 487
        Essentially acts as a class method decorator
        '''
        super().__init_subclass__(**kwargs)

    @property
    def name(self):
        '''
        Algorithm name
        '''
        return self.__name

    @property
    def hbook(self):
        '''
        histogram collection
        '''
        return self._hbook

    @hbook.setter
    def hbook(self, hbook):
        self._hbook = hbook

    @staticmethod
    def load(logger, **kwargs):
        '''
        Returns the class instance from a dictionary
        '''
        logger.info('Loading Algo %s' % kwargs['name'])
        try:
            module = importlib.import_module(
                    kwargs['module']
                    )
        except ImportError:
            logger.error('Unable to load module %s' % kwargs['module'])
            raise
        except Exception as e:
            logger.error("Unknow error loading module: %s" % e)
            raise
        try:
            class_ = getattr(module, kwargs['class'])
        except AttributeError:
            logger.error("%s: missing attribute %s" %
                         (kwargs['name'], kwargs['class']))
            raise
        except Exception as e:
            logger.error("Reason: %s" % e)
            raise

        logger.debug(pformat(kwargs['properties']))

        # Update the logging level of
        # algorithms if loglevel not set
        # Ensures user-defined algos get the artemis level logging
        if 'loglevel' not in kwargs['properties']:
            kwargs['properties']['loglevel'] = \
                    logger.getEffectiveLevel()

        try:
            instance = class_(kwargs['name'], **kwargs['properties'])
        except AttributeError:
            logger.error("%s: missing attribute %s" %
                         (kwargs['name'], 'properties'))
            raise
        except Exception as e:
            logger.error("%s: Cannot initialize %s" % e)
            raise

        return instance

    def to_dict(self):
        '''
        Create json-serialize class
        to create the algorithm from all properties

        name - instance name as found in menu
        module - where the class algo resides
        class - concrete class name
        properties - all the user-defined properties
        '''
        _dict = OrderedDict()
        _dict['name'] = self.name
        _dict['class'] = self.__class__.__name__
        _dict['module'] = self.__module__
        _dict['properties'] = self.properties.to_dict()

        return _dict

    def to_msg(self):
        message = Algo_pb()
        message.name = self.name
        message.klass = self.__class__.__name__
        message.module = self.__module__
        message.properties.CopyFrom(self.properties.to_msg())
        return message

    @staticmethod
    def from_msg(logger, msg):
        logger.info('Loading Algo from msg %s', msg.name)
        try:
            module = importlib.import_module(msg.module)
        except ImportError:
            logger.error('Unable to load module %s', msg.module)
            raise
        except Exception as e:
            logger.error("Unknow error loading module: %s" % e)
            raise
        try:
            class_ = getattr(module, msg.klass)
        except AttributeError:
            logger.error("%s: missing attribute %s" %
                         (msg.name, msg.klass))
            raise
        except Exception as e:
            logger.error("Reason: %s" % e)
            raise

        properties = Properties.from_msg(msg.properties)
        logger.debug(pformat(properties))

        # Update the logging level of
        # algorithms if loglevel not set
        # Ensures user-defined algos get the artemis level logging
        if 'loglevel' not in properties:
            properties['loglevel'] = \
                    logger.getEffectiveLevel()

        try:
            instance = class_(msg.name, **properties)
        except AttributeError:
            logger.error("%s: missing attribute %s" %
                         (msg.name, 'properties'))
            raise
        except Exception as e:
            logger.error("%s: Cannot initialize %s" % e)
            raise

        return instance

    def lock(self):
        '''
        Lock all properties for algorithm
        '''
        self.properties.lock = True

    def initialize(self):
        '''
        Framework initialize
        '''
        pass

    def book(self):
        '''
        Book histograms
        '''
        pass

    def rebook(self):
        '''
        Rebook with new binnings
        '''
        pass

    def execute(self, payload):
        '''
        Algo always accepts the output Node on a graph
        Data is accessed via the Parent.payload
        '''
        pass

    def finalize(self):
        '''
        report timings, counters, etc..
        '''
        pass
