#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Algorithms
"""
import logging
from collections import OrderedDict

from artemis.logger import Logger
from artemis.core.properties import Properties
from artemis.decorators import timethis

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

        self.__logger.info('__init__ AlgoBase')
        print('__init__ AlgoBase')
        # name will be mangled to _AlgoBase__name
        self.__name = name
        self.properties = Properties()
        for key in kwargs:
            self.properties.add_property(key, kwargs[key])

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
    
    @timethis
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
