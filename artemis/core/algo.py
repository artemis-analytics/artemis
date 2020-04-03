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
Algorithms
"""
from collections import OrderedDict
import importlib
from pprint import pformat

from artemis.logger import Logger
from artemis.core.abcalgo import AbcAlgoBase
from artemis.core.properties import Properties
from artemis.core.gate import ArtemisGateSvc
from artemis_format.pymodels.configuration_pb2 import Module as Algo_pb

from artemis.core.gate import IOMetaMixin, MetaMixin

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


class AlgoBase(MetaMixin, metaclass=AbcAlgoBase):
    def __init__(self, name, **kwargs):
        """
        Access the Base logger directly through
        self.__logger
        Derived class use the classmethods for info, debug, warn, error
        All formatting, loglevel checks, etc...
        can be done through the classmethods

        Can we use staticmethods in artemis to make uniform
        formatting of info, debug, warn, error?
        """
        # Configure logging
        Logger.configure(self, **kwargs)

        self.__logger.debug("__init__ AlgoBase")
        # name will be mangled to _AlgoBase__name
        self.__name = name
        self.properties = Properties()
        for key in kwargs:
            self.properties.add_property(key, kwargs[key])

        self.gate = ArtemisGateSvc()

    def __init_subclass__(cls, **kwargs):
        """
        See PEP 487
        Essentially acts as a class method decorator
        """
        super().__init_subclass__(**kwargs)

    @property
    def name(self):
        """
        Algorithm name
        """
        return self.__name

    @staticmethod
    def load(logger, **kwargs):
        """
        Returns the class instance from a dictionary
        """
        logger.info("Loading Algo %s" % kwargs["name"])
        try:
            module = importlib.import_module(kwargs["module"])
        except ImportError:
            logger.error("Unable to load module %s" % kwargs["module"])
            raise
        except Exception as e:
            logger.error("Unknow error loading module: %s" % e)
            raise
        try:
            class_ = getattr(module, kwargs["class"])
        except AttributeError:
            logger.error("%s: missing attribute %s" % (kwargs["name"], kwargs["class"]))
            raise
        except Exception as e:
            logger.error("Reason: %s" % e)
            raise

        logger.debug(pformat(kwargs["properties"]))

        # Update the logging level of
        # algorithms if loglevel not set
        # Ensures user-defined algos get the artemis level logging
        if "loglevel" not in kwargs["properties"]:
            kwargs["properties"]["loglevel"] = logger.getEffectiveLevel()

        try:
            instance = class_(kwargs["name"], **kwargs["properties"])
        except AttributeError:
            logger.error("%s: missing attribute %s" % (kwargs["name"], "properties"))
            raise
        except Exception as e:
            logger.error("%s: Cannot initialize %s" % e)
            raise

        return instance

    def to_dict(self):
        """
        Create json-serialize class
        to create the algorithm from all properties

        name - instance name as found in menu
        module - where the class algo resides
        class - concrete class name
        properties - all the user-defined properties
        """
        _dict = OrderedDict()
        _dict["name"] = self.name
        _dict["class"] = self.__class__.__name__
        _dict["module"] = self.__module__
        _dict["properties"] = self.properties.to_dict()

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
        logger.info("Loading Algo from msg %s", msg.name)
        try:
            module = importlib.import_module(msg.module)
        except ImportError:
            logger.error("Unable to load module %s", msg.module)
            raise
        except Exception as e:
            logger.error("Unknow error loading module: %s" % e)
            raise
        try:
            class_ = getattr(module, msg.klass)
        except AttributeError:
            logger.error("%s: missing attribute %s" % (msg.name, msg.klass))
            raise
        except Exception as e:
            logger.error("Reason: %s" % e)
            raise

        properties = Properties.from_msg(msg.properties)
        logger.debug(pformat(properties))

        # Update the logging level of
        # algorithms if loglevel not set
        # Ensures user-defined algos get the artemis level logging
        if "loglevel" not in properties:
            properties["loglevel"] = logger.getEffectiveLevel()

        try:
            instance = class_(msg.name, **properties)
        except AttributeError:
            logger.error("%s: missing attribute %s" % (msg.name, "properties"))
            raise
        except Exception as e:
            logger.error("%s: Cannot initialize %s" % e)
            raise

        return instance

    def lock(self):
        """
        Lock all properties for algorithm
        """
        self.properties.lock = True

    def initialize(self):
        """
        Framework initialize
        """
        raise NotImplementedError

    def book(self):
        """
        Book histograms
        """
        raise NotImplementedError

    def rebook(self):
        """
        Rebook with new binnings
        """
        raise NotImplementedError

    def execute(self, payload):
        """
        Algo always accepts the output Node on a graph
        Data is accessed via the Parent.payload
        """
        raise NotImplementedError

    def finalize(self):
        """
        report timings, counters, etc..
        """
        raise NotImplementedError


class IOAlgoBase(MetaMixin, IOMetaMixin, metaclass=AbcAlgoBase):
    def __init__(self, name, **kwargs):
        """
        Access the Base logger directly through
        self.__logger
        Derived class use the classmethods for info, debug, warn, error
        All formatting, loglevel checks, etc...
        can be done through the classmethods

        Can we use staticmethods in artemis to make uniform
        formatting of info, debug, warn, error?
        """
        # Configure logging
        Logger.configure(self, **kwargs)

        self.__logger.debug("__init__ AlgoBase")
        # name will be mangled to _AlgoBase__name
        self.__name = name
        self.properties = Properties()
        for key in kwargs:
            self.properties.add_property(key, kwargs[key])

        self.gate = ArtemisGateSvc()

    @property
    def name(self):
        """
        Algorithm name
        """
        return self.__name

    @staticmethod
    def load(logger, **kwargs):
        """
        Returns the class instance from a dictionary
        """
        logger.info("Loading Algo %s" % kwargs["name"])
        try:
            module = importlib.import_module(kwargs["module"])
        except ImportError:
            logger.error("Unable to load module %s" % kwargs["module"])
            raise
        except Exception as e:
            logger.error("Unknow error loading module: %s" % e)
            raise
        try:
            class_ = getattr(module, kwargs["class"])
        except AttributeError:
            logger.error("%s: missing attribute %s" % (kwargs["name"], kwargs["class"]))
            raise
        except Exception as e:
            logger.error("Reason: %s" % e)
            raise

        logger.debug(pformat(kwargs["properties"]))

        # Update the logging level of
        # algorithms if loglevel not set
        # Ensures user-defined algos get the artemis level logging
        if "loglevel" not in kwargs["properties"]:
            kwargs["properties"]["loglevel"] = logger.getEffectiveLevel()

        try:
            instance = class_(kwargs["name"], **kwargs["properties"])
        except AttributeError:
            logger.error("%s: missing attribute %s" % (kwargs["name"], "properties"))
            raise
        except Exception as e:
            logger.error("%s: Cannot initialize %s" % e)
            raise

        return instance

    def to_dict(self):
        """
        Create json-serialize class
        to create the algorithm from all properties

        name - instance name as found in menu
        module - where the class algo resides
        class - concrete class name
        properties - all the user-defined properties
        """
        _dict = OrderedDict()
        _dict["name"] = self.name
        _dict["class"] = self.__class__.__name__
        _dict["module"] = self.__module__
        _dict["properties"] = self.properties.to_dict()

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
        logger.info("Loading Algo from msg %s", msg.name)
        try:
            module = importlib.import_module(msg.module)
        except ImportError:
            logger.error("Unable to load module %s", msg.module)
            raise
        except Exception as e:
            logger.error("Unknow error loading module: %s" % e)
            raise
        try:
            class_ = getattr(module, msg.klass)
        except AttributeError:
            logger.error("%s: missing attribute %s" % (msg.name, msg.klass))
            raise
        except Exception as e:
            logger.error("Reason: %s" % e)
            raise

        properties = Properties.from_msg(msg.properties)
        logger.debug(pformat(properties))

        # Update the logging level of
        # algorithms if loglevel not set
        # Ensures user-defined algos get the artemis level logging
        if "loglevel" not in properties:
            properties["loglevel"] = logger.getEffectiveLevel()

        try:
            instance = class_(msg.name, **properties)
        except AttributeError:
            logger.error("%s: missing attribute %s" % (msg.name, "properties"))
            raise
        except Exception as e:
            logger.error("%s: Cannot initialize %s" % e)
            raise

        return instance

    def lock(self):
        """
        Lock all properties for algorithm
        """
        self.properties.lock = True

    def initialize(self):
        """
        Framework initialize
        """
        raise NotImplementedError

    def book(self):
        """
        Book histograms
        """
        raise NotImplementedError

    def rebook(self):
        """
        Rebook with new binnings
        """
        raise NotImplementedError

    def execute(self, payload):
        """
        Algo always accepts the output Node on a graph
        Data is accessed via the Parent.payload
        """
        raise NotImplementedError

    def finalize(self):
        """
        report timings, counters, etc..
        """
        raise NotImplementedError
